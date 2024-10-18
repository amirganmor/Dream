import os
from datetime import datetime, timedelta
# Configuration settings for extract Lambda functions

BUCKET_NAME = os.getenv('BUCKET_NAME', 'your-raw-bucket')  # Raw S3 bucket name
MAX_CHUNK_SIZE = int(os.getenv('MAX_CHUNK_SIZE', '500'))   # Max chunk size for splitting data
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL', 'your-sqs-queue-url')  # SQS Queue URL
API_URL = os.getenv('API_URL', 'https://jsonplaceholder.typicode.com/users')  # API URL


# Configuration settings for the pre-transform Lambda function
RAW_BUCKET_NAME = os.getenv('RAW_BUCKET_NAME', 'your-raw-bucket')  # Raw S3 bucket name
OUTPUT_BUCKET = os.getenv('OUTPUT_BUCKET', 'your-output-bucket')  # Output S3 bucket name
AGGREGATED_BUCKET = os.getenv('AGGREGATED_BUCKET', 'your-aggregated-bucket')  # Aggregated data bucket
EMR_JOB_FLOW_ID = os.getenv('EMR_JOB_FLOW_ID', 'j-XXXXXXXX')  # EMR cluster ID
TRANSFORM_SCRIPT_PATH = os.getenv('TRANSFORM_SCRIPT_PATH', 's3://your-script-bucket/transform.py')  # Path to the Spark transform script


# Configuration settings for the transform.py script
FINAL_BUCKET_NAME = os.getenv('FINAL_BUCKET_NAME', 'your-final-data-bucket')  # Final S3 bucket name
AGGREGATED_BUCKET_NAME = os.getenv('AGGREGATED_BUCKET_NAME', 'your-aggregated-bucket')  # Aggregated S3 bucket name
DEMO_API_URL = os.getenv('DEMO_API_URL', 'https://api.mock.com/demographics/')  # API URL for fetching demographics
SENTIMENT_ANALYSIS_API_URL = os.getenv('SENTIMENT_ANALYSIS_API_URL', 'https://api.mock.com/sentiment')  # Mock Sentiment API

# Configuration settings for the loading.py script
# AWS SQS Configurations
SQS_QUEUE_URL_SNOWFLAKE = os.getenv('SQS_QUEUE_URL')
# Snowflake Configurations
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')


#airflow Vars
# AWS configuration
AWS_CONN_ID = 'aws_default'

# S3 Keys
RAW_KEY_PATTERN = 'raw/input_file.json'
SPARK_OUTPUT_KEY_PATTERN = f'final_data/{{{{ ds }}}}/{{{{ task_instance_key_str }}}}.json'  # Airflow templating
SNOWFLAKE_CONFIRMATION_KEY_PATTERN = f'confirmation_file/{{{{ ds }}}}/*.json'

# Lambda function names
LAMBDA_EXACT = 'exact'
LAMBDA_TRANSFORM_1 = 'spark_transform_1'
LAMBDA_TRANSFORM_2 = 'spark_transform_2'
LAMBDA_SNOWFLAKE_LOAD = 'load_data_to_snowflake'

# DAG Arguments
DEFAULT_ARGS = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 10, 1),
}