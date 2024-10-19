# ETL Pipeline with AWS, Apache Airflow, and Snowflake

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline that extracts data from an external API, transforms it using AWS services like Lambda and EMR, and loads it into a Snowflake data warehouse. The pipeline is orchestrated using Apache Airflow.

### Key Technologies
- **AWS Lambda**: Used for data extraction and triggering the transformation jobs.
- **Amazon S3**: Stores raw and transformed data.
- **Amazon SQS**: Handles event messaging.
- **Amazon EMR**: Executes the data transformation jobs using Spark.
- **Snowflake**: Serves as the data warehouse for storing transformed data.
- **Apache Airflow**: Manages and orchestrates the workflow of the entire pipeline.

---

## Project Structure

dream/
├── README.md               # Project documentation (this file)
├── airflow_dag.py          # Airflow DAG for pipeline orchestration
├── requirements.txt        # Python dependencies
├── config.py               # Project configuration and settings
├── extract.py              # Logic for extracting data from an external API
├── pre_transform.py        # Pre-transformation data validation and cleanup
├── transform.py            # Spark-based data transformation logic
├── loading.py              # Logic for loading transformed data into Snowflake
├── tests/                  # Unit and integration tests
│   ├── integration/
│   │     └── integration_test.py  # Integration tests for the whole pipeline
│   ├── unit/
│        ├── extract_test.py      # Unit tests for extract.py
│        ├── pre_transform_test.py # Unit tests for pre_transform.py
│        ├── transform_test.py    # Unit tests for transform.py
│        └── loading_test.py      # Unit tests for loading.py
└── .github/workflows/test.yml    # GitHub Actions configuration for continuous integration (CI) testing

# ETL Pipeline Set Up

## Overview

This document provides detailed instructions on setting up and running an ETL pipeline using AWS services, including S3, Lambda, EMR, Snowflake, and Apache Airflow. The pipeline extracts JSON data, transforms it, and loads it into a data warehouse for analytics.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Architecture Overview](#architecture-overview)
3. [Deployment Instructions](#deployment-instructions)
   - [Step 1: Set Up AWS Infrastructure](#step-1-set-up-aws-infrastructure)
   - [Step 2: Deploy Lambda Functions](#step-2-deploy-lambda-functions)
   - [Step 3: Configure Amazon EMR](#step-3-configure-amazon-emr)
   - [Step 4: Set Up Snowflake](#step-4-set-up-snowflake)
   - [Step 5: Configure Apache Airflow](#step-5-configure-apache-airflow)
4. [Running the Pipeline](#running-the-pipeline)
5. [Monitoring and Logging](#monitoring-and-logging)
6. [Using Docker](#using-docker)

## Prerequisites

- AWS account with IAM permissions to create S3 buckets, Lambda functions, EMR clusters, and other resources.
- Python 3.x and AWS CLI installed.
- Docker installed (optional for local development/testing).
- Snowflake account for data warehousing.
- Apache Airflow installed (if running locally).

## Architecture Overview

The ETL pipeline consists of the following components:

- **S3 Bucket**: For storing raw and transformed data.
- **AWS Lambda**: For extracting data and triggering processing.
- **Amazon SQS**: For queuing file events.
- **Amazon EMR**: For processing data with Spark.
- **Snowflake**: For data warehousing.
- **Apache Airflow**: For orchestrating the workflow.

## Deployment Instructions

### Step 1: Set Up AWS Infrastructure

1. **Create S3 Buckets:**

   Create two S3 buckets: one for raw data and another for transformed data.
   bash
   aws s3api create-bucket --bucket your-raw-bucket --region us-east-1
   aws s3api create-bucket --bucket your-transformed-bucket --region us-east-1

2. **Create IAM Roles**
Create IAM roles for Lambda and EMR with necessary permissions to access S3 and other AWS services.

### Step 2: Deploy Lambda Functions
1. **Create Lambda Function for Data Extraction**
Create a new Lambda function and configure it with the code from your extract.py.
Set environment variables such as BUCKET_NAME and MAX_CHUNK_SIZE.
2. **Create Lambda Function for EMR Trigger**
Create another Lambda function to process SQS messages and submit jobs to the EMR cluster using your pre_transform.py. This function will also reference the Spark job defined in transform.py.
Set environment variables as necessary.
3. **Deploy Lambda Functions**
Package the Lambda code and dependencies in a ZIP file.

bash code
zip -r lambda_extraction.zip extract.py
zip -r lambda_trigger.zip pre_transform.py

Deploy the Lambda functions:

bash code
aws lambda create-function --function-name DataExtractionFunction --runtime python3.x --role arn:aws:iam::account-id:role/your-lambda-role --handler extract.lambda_handler --zip-file fileb://lambda_extraction.zip

aws lambda create-function --function-name EMRTriggerFunction --runtime python3.x --role arn:aws:iam::account-id:role/your-lambda-role --handler pre_transform.lambda_handler --zip-file fileb://lambda_trigger.zip
### Step 3: Configure Amazon EMR
1. **Create EMR Cluster**
Launch an EMR cluster with the necessary configurations (e.g., instance types, number of nodes) and install Spark to run your transform.py script.

bash code
aws emr create-cluster --name "ETL Cluster" --release-label emr-6.x.x --applications Name=Spark --ec2-attributes KeyName=your-key --instance-type m5.xlarge --instance-count 3
2. **Upload Spark Job Script**
Upload your Spark transformation script (transform.py) to an S3 bucket for access by the EMR cluster during execution.

bash code
aws s3 cp transform.py s3://your-script-bucket/

Additional Notes
Ensure that pre_transform.py invokes the Spark job using the path to transform.py uploaded in Step 3.2.
Replace placeholders like your-raw-bucket, your-transformed-bucket, account-id, your-lambda-role, your-key, and your-script-bucket with your actual resource names and identifiers.
Confirm that your IAM roles have appropriate permissions for S3, Lambda, and EMR operations.
### Step 4: Set Up Snowflake
1. Create Snowflake Warehouse and Database
Log into Snowflake and create a warehouse, database, and schema as needed.

sql code
CREATE WAREHOUSE your_warehouse WITH WAREHOUSE_SIZE='SMALL' AUTO_RESUME=true;
CREATE DATABASE your_database;
CREATE SCHEMA your_schema;

2. **Set Up Snowflake Credentials**
Ensure that your Lambda function has the necessary environment variables for connecting to Snowflake.

### Step 5: Configure Apache Airflow
1. **Install Apache Airflow**
Follow the official documentation to install and set up Airflow locally or on a server.

2. **DAG for Orchestration**
Define a DAG in Airflow that orchestrates the entire ETL workflow. The DAG will trigger the Lambda functions and monitor their execution. The DAG definition should be in the file located at airflow_dag.py. Ensure it includes tasks for:

Triggering the Data Extraction Lambda function.
Monitoring SQS messages to initiate EMR processing.
Triggering the EMR jobs using the Spark script for data transformation.
Loading the transformed data into Snowflake.

### Running the Pipeline
Trigger Data Extraction: Manually invoke the data extraction Lambda function or set it up with a scheduled event trigger.
Process SQS Messages: As data is added to the S3 bucket, messages will be sent to the SQS queue. The EMR trigger Lambda will automatically process these messages.
Transform Data with EMR: EMR will process the data according to the defined Spark jobs.
Load Data into Snowflake: Ensure that transformed data is loaded into the designated Snowflake tables.
Monitor Workflow in Airflow: Use the Airflow UI to monitor the status of tasks in your ETL pipeline.

sql code
This file includes all the steps in one cohesive document. Let me know if you need any further changes!
