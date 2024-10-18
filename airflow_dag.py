# dag.py

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.lambda_function import AWSLambdaInvokeFunctionOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import json
import config

# Failure callback function
def on_failure_callback(context):
    # Add your alerting logic here, e.g., email or Slack notification
    print(f"Task {context['task_instance_key_str']} failed. Please investigate!")

# Define the DAG
dag = DAG(
    'enhanced_etl_pipeline_with_lambda',
    default_args=config.DEFAULT_ARGS,
    schedule_interval=None,  # Event-driven triggering
)

# Task 1: Trigger the first Lambda function named 'exact'
invoke_lambda_task = AWSLambdaInvokeFunctionOperator(
    task_id='invoke_lambda',
    function_name=config.LAMBDA_EXACT,  # The name of your first Lambda function
    aws_conn_id=config.AWS_CONN_ID,  # Use the connection from config
    log_type='Tail',
    payload=json.dumps({
        'bucket': config.RAW_BUCKET_NAME,
        'key': config.RAW_KEY_PATTERN
    }),
    dag=dag
)

# Task 2: Wait for the file in S3
wait_for_file = S3KeySensor(
    task_id='wait_for_file',
    bucket_name=config.RAW_BUCKET_NAME,
    bucket_key=f'raw/{{ ds }}/*.json',  # Use Airflow templating for date
    aws_conn_id=config.AWS_CONN_ID,
    poke_interval=300,
    timeout=3600,
    dag=dag,
)

# Task 3: Trigger parallel Lambda transformations
with TaskGroup('lambda_transformations') as lambda_transformations:
    
    # Lambda 1: Transformation Step
    invoke_lambda_transform_1 = AWSLambdaInvokeFunctionOperator(
        task_id='invoke_lambda_transform_1',
        function_name=config.LAMBDA_TRANSFORM_1,
        aws_conn_id=config.AWS_CONN_ID,
        log_type='Tail',
        payload=json.dumps({
            'bucket': config.RAW_BUCKET_NAME,
            'key': config.RAW_KEY_PATTERN
        }),
        dag=dag,
    )

    # Lambda 2: Another transformation
    invoke_lambda_transform_2 = AWSLambdaInvokeFunctionOperator(
        task_id='invoke_lambda_transform_2',
        function_name=config.LAMBDA_TRANSFORM_2,
        aws_conn_id=config.AWS_CONN_ID,
        log_type='Tail',
        payload=json.dumps({
            'bucket': config.RAW_BUCKET_NAME,
            'key': config.RAW_KEY_PATTERN
        }),
        dag=dag,
    )

# Task 4: Wait for output data from the Spark job
wait_for_spark_output = S3KeySensor(
    task_id='wait_for_spark_output',
    bucket_name=config.OUTPUT_BUCKET,
    bucket_key=config.SPARK_OUTPUT_KEY_PATTERN,
    aws_conn_id=config.AWS_CONN_ID,
    poke_interval=300,
    timeout=3600,
    dag=dag,
)

# Task 5: Invoke the Lambda function to load data into Snowflake
invoke_lambda_snowflake_load_task = AWSLambdaInvokeFunctionOperator(
    task_id='invoke_lambda_snowflake_load',
    function_name=config.LAMBDA_SNOWFLAKE_LOAD,
    aws_conn_id=config.AWS_CONN_ID,
    log_type='Tail',
    payload=json.dumps({
        'bucket': config.OUTPUT_BUCKET,
        'key': config.SPARK_OUTPUT_KEY_PATTERN
    }),
    dag=dag,
)

# Task 6: (Optional) Wait for a final confirmation file in S3
wait_for_snowflake_confirmation = S3KeySensor(
    task_id='wait_for_snowflake_confirmation',
    bucket_name=config.OUTPUT_BUCKET,
    bucket_key=config.SNOWFLAKE_CONFIRMATION_KEY_PATTERN,
    aws_conn_id=config.AWS_CONN_ID,
    poke_interval=300,
    timeout=3600,
    dag=dag,
)

# Set task dependencies
invoke_lambda_task >> wait_for_file >> lambda_transformations >> wait_for_spark_output >> invoke_lambda_snowflake_load_task >> wait_for_snowflake_confirmation
