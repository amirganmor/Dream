from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, regexp_replace, count
import requests
import logging
import config  # Import the config file

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName('ETL-Transformation') \
    .getOrCreate()

# Read raw data from S3
def read_raw_data(s3_path):
    try:
        raw_data = spark.read.option("multiline", "true").json(s3_path)
        logger.info(f"Successfully read data from {s3_path}")
        return raw_data
    except Exception as e:
        logger.error(f"Error reading raw data from {s3_path}: {str(e)}")
        raise

# Transform the data
def transform_data(raw_data):
    transformed_data = (
        raw_data
        .filter(col('email').isNotNull())  # Remove invalid email
        .withColumn('id', col('id').cast('string'))  # Convert 'id' to string
        .withColumn('domain', regexp_replace(col('email'), r".+@", ""))  # Extract domain
        .withColumn('full_address',
            concat_ws(", ", 
                col('address.street'), 
                col('address.suite'), 
                col('address.city'), 
                col('address.zipcode')
            )  # Create 'full_address' using nested fields
        )
        .withColumn('company_name', col('company.name'))  # Extract 'company.name' as top-level column
        # Standardize phone numbers: remove special characters and add country code
        .withColumn('phone', 
            regexp_replace(col('phone'), r'[\(\)\s\-\.]', '')  # Remove special characters
        )
        .withColumn('phone', 
            regexp_replace(col('phone'), r'^(\d{10})$', r'+1-\1')  # Add "+1-" for U.S. numbers
        )
    )
    return transformed_data

# Function to fetch demographics
def fetch_demographics(zipcodes):
    try:
        if isinstance(zipcodes, list):  # If multiple zip codes
            responses = []
            for zipcode in zipcodes:
                response = requests.get(f"{config.DEMO_API_URL}{zipcode}")  # Use URL from config
                responses.append(response.json().get("demographics", "Unknown"))
            return responses
        else:  # Single zip code
            response = requests.get(f"{config.DEMO_API_URL}{zipcodes}")  # Use URL from config
            return response.json().get("demographics", "Unknown")
    except Exception as e:
        logger.error(f"Error fetching demographics: {str(e)}")
        return "Unknown"

# Enrich data with demographics
def enrich_demographics(transformed_data):
    try:
        # Collect unique zip codes for bulk processing
        zipcodes = transformed_data.select('address.zipcode').distinct().rdd.flatMap(lambda x: x).collect()
        
        # Fetch demographics for all zip codes
        demographics = fetch_demographics(zipcodes)

        # Create a mapping for demographics
        zipcode_to_demo = dict(zip(zipcodes, demographics))

        # Create a new column with demographics using mapping
        demographics_udf = spark.udf.register("get_demographics", lambda zipcode: zipcode_to_demo.get(zipcode, "Unknown"))
        transformed_data = transformed_data.withColumn('demographics', demographics_udf(col('address.zipcode')))
        
        logger.info("Successfully enriched data with demographics")
        return transformed_data

    except Exception as e:
        logger.error(f"Error enriching demographics: {str(e)}")
        raise

# Add sentiment analysis mock
def sentiment_analysis(catch_phrase):
    return "positive"  # Mock sentiment response

sentiment_udf = spark.udf.register("sentiment_analysis", sentiment_analysis)

# Main transformation workflow
def main(s3_input_path, s3_output_path, s3_aggregated_path):
    try:
        raw_data = read_raw_data(s3_input_path)
        transformed_data = transform_data(raw_data)
        transformed_data = enrich_demographics(transformed_data)
        transformed_data = transformed_data.withColumn("sentiment", sentiment_udf(col("company.catchPhrase")))

        # Filter users by business rules
        final_data = (
            transformed_data
            .filter(col('username').rlike(r'^.{5,}$'))  # Filter usernames less than 5 characters
            .filter(col('sentiment') != "negative")  # Exclude negative sentiments
        )

        # Aggregation
        aggregated_data = final_data.groupBy('company_name').agg(count('id').alias('user_count'))

        # Write the transformed data in Parquet format
        final_data.write.mode("overwrite").parquet(s3_output_path)
        aggregated_data.write.mode("overwrite").partitionBy('company_name').parquet(s3_aggregated_path)

        logger.info("Data transformation and writing completed successfully.")
    
    except Exception as e:
        logger.error(f"Error in the main workflow: {str(e)}")
        raise

# Main entry point
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: spark-submit my_script.py <s3_input_path> <s3_output_path> <s3_aggregated_path>")
        sys.exit(1)

    s3_input_path = sys.argv[1]  # e.g., s3://{config.RAW_BUCKET_NAME}/raw/
    s3_output_path = sys.argv[2]  # e.g., s3://{config.FINAL_BUCKET_NAME}/final_data/
    s3_aggregated_path = sys.argv[3]  # e.g., s3://{config.AGGREGATED_BUCKET_NAME}/aggregated_data/

    main(s3_input_path, s3_output_path, s3_aggregated_path)
