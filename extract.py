import json
import boto3
import requests
import logging
from botocore.exceptions import ClientError
from datetime import datetime
import config  # Import the config file

# Initialize S3 and SQS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

# Logger configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Fetch the API URL from config
    api_url = config.API_URL
    
    try:
        # Fetch data from API
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data:
            logger.warning("No data returned from API.")
            return
        
        logger.info(f"Fetched {len(data)} records from the API.")

        # Split data into chunks if too large
        if len(data) > 1000:
            logger.info(f"Data too large, splitting into chunks of {config.MAX_CHUNK_SIZE}")
            for i in range(0, len(data), config.MAX_CHUNK_SIZE):
                chunk = data[i:i+config.MAX_CHUNK_SIZE]
                _save_data_to_s3(chunk, i)
                _send_sqs_message(f"raw_data_{i}.json")  # Send message for each chunk
        else:
            _save_data_to_s3(data)
            _send_sqs_message("raw_data.json")  # Send message for the single file
    
    except requests.exceptions.Timeout:
        logger.error("API request timed out.")
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
    except ClientError as e:
        logger.error(f"S3 operation failed: {str(e)}")

def _save_data_to_s3(data, chunk_index=None):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        if chunk_index is not None:
            file_name = f"raw_data_{timestamp}_chunk_{chunk_index}.json"
        else:
            file_name = f"raw_data_{timestamp}.json"

        s3.put_object(
            Bucket=config.BUCKET_NAME,  # Use the bucket name from config
            Key=f"raw/{file_name}",
            Body=json.dumps(data),
            ContentType='application/json'
        )
        logger.info(f"Data saved to S3: {file_name}")
    except ClientError as e:
        logger.error(f"Failed to save data to S3: {str(e)}")
        raise e  # Re-raise to handle retries or alerting

def _send_sqs_message(file_name):
    """Send a message to SQS to notify that a new file is ready for processing."""
    try:
        message_body = json.dumps({"file_name": file_name})
        sqs.send_message(QueueUrl=config.SQS_QUEUE_URL, MessageBody=message_body)  # Use SQS URL from config
        logger.info(f"Sent SQS message for file: {file_name}")
    except ClientError as e:
        logger.error(f"Failed to send SQS message: {str(e)}")
