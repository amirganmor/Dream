import boto3
import json
import logging
import config  # Import the config file
from botocore.exceptions import ClientError

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize EMR client
emr_client = boto3.client('emr')

# Batching helper function
def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def lambda_handler(event, context):
    # List of S3 file keys from the incoming SQS event
    keys = []
    for record in event['Records']:
        body = json.loads(record['body'])
        keys.append(body['file_name'])

    try:
        # Submit EMR jobs in batches to avoid too many concurrent jobs
        for key_batch in batch(keys, 10):  # Batch 10 files per job
            args = ['spark-submit', config.TRANSFORM_SCRIPT_PATH]  # Use path from config
            
            for key in key_batch:
                args.append(f"s3://{config.RAW_BUCKET_NAME}/raw/{key}")  # Use bucket name from config

            args.extend([
                f"s3://{config.OUTPUT_BUCKET}/final_data/",  # Output path from config
                f"s3://{config.AGGREGATED_BUCKET}/aggregated_data/"  # Aggregated output path from config
            ])

            # Submit Spark job to EMR for batch of files
            response = emr_client.add_job_flow_steps(
                JobFlowId=config.EMR_JOB_FLOW_ID,  # Use EMR cluster ID from config
                Steps=[
                    {
                        'Name': 'ETL Job for Batch',
                        'ActionOnFailure': 'CONTINUE',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': args
                        }
                    }
                ]
            )
            logger.info(f"Submitted Spark job for batch: {response['StepIds']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Spark jobs submitted successfully!')
        }
    
    except Exception as e:
        logger.error(f"Failed to submit Spark job: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
