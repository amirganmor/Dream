import json
import logging
import boto3
import snowflake.connector
import concurrent.futures
import time
import random
from config import SQS_QUEUE_URL_SNOWFLAKE, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize SQS and S3 clients
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    logger.info("Lambda function triggered.")
    
    # Fetch batch of messages from SQS
    messages = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL_SNOWFLAKE,
        MaxNumberOfMessages=10,  # Adjust batch size for efficiency
        WaitTimeSeconds=5
    )
    
    if 'Messages' not in messages:
        logger.info("No messages in SQS.")
        return {'statusCode': 200, 'body': 'No new files to process.'}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_message, message)
            for message in messages['Messages']
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Check if the process raised any exception
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")

    return {'statusCode': 200, 'body': 'Processing complete.'}

def process_message(message):
    try:
        # Extract file info from the message
        file_event = message['Body']
        logger.info(f"Processing file event: {file_event}")
        
        # Parse the S3 event message
        file_info = parse_s3_event(file_event)
        bucket = file_info['bucket']
        key = file_info['key']

        # Initiate Snowflake connection
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )

        # Load data into Snowflake based on the file type
        if 'transformed_data' in key:
            load_data_to_snowflake(conn, bucket, key, "FINAL_TRANSFORMED_TABLE")
        elif 'aggregated_data' in key:
            load_data_to_snowflake(conn, bucket, key, "AGGREGATED_COMPANY_DATA")

        # Delete the message after successful processing
        sqs.delete_message(QueueUrl=SQS_QUEUE_URL_SNOWFLAKE, ReceiptHandle=message['ReceiptHandle'])

    except Exception as e:
        logger.error(f"Error processing file event: {e}")

def parse_s3_event(event_body):
    """Parse S3 event message and extract relevant details."""
    # Extracting bucket name and file key from the event (for simplicity)
    # Add necessary parsing logic based on S3 event structure
    event = json.loads(event_body)  # Assuming the body is JSON encoded
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    return {
        'bucket': bucket,
        'key': key
    }

def load_data_to_snowflake(conn, bucket, key, table_name):
    """Load data from S3 to Snowflake."""
    s3_path = f"s3://{bucket}/{key}"
    
    copy_sql = f"""
    COPY INTO {table_name}
    FROM '{s3_path}'
    FILE_FORMAT = (TYPE = 'PARQUET')
    ON_ERROR = 'CONTINUE';
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(copy_sql)
            logger.info(f"Data from {s3_path} loaded into {table_name}.")
            row_count = cur.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Rows loaded into {table_name}: {row_count}")
    except Exception as e:
        logger.error(f"Error loading data into {table_name}: {e}")
    finally:
        conn.close()

def exponential_backoff(retries):
    """Implements exponential backoff with jitter."""
    sleep_time = min(2 ** retries + random.uniform(0, 1), 60)  # Max wait time of 60 seconds
    time.sleep(sleep_time)
