import os
import sys
from pyspark.sql import SparkSession

# -------------------------------------------------------------------------
# INSTRUCTIONS FOR RUNNING
# -------------------------------------------------------------------------
# Run this script using the following spark-submit command:
#
# spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 s3_pyspark_pipeline.py
#
# Note: The --packages option automatically downloads the required JARs.
# If you prefer manual JARs, download them from Maven Central and use --jars.
# -------------------------------------------------------------------------

# AWS Region
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")

def get_s3_path(key):
    """Returns the full S3A path for use with PySpark."""
    # Strip leading slashes to avoid double slashes
    clean_key = key.lstrip('/')
    return f"s3a://{S3_BUCKET_NAME}/{clean_key}"

# -------------------------------------------------------------------------
# 2. PySpark Session Setup with Hadoop-AWS Support
# -------------------------------------------------------------------------
def create_spark_session():
    """
    Creates a SparkSession configured for local execution and S3 access.
    """
    # Maven coordinates for Hadoop AWS and AWS SDK
    # Standard for Spark 3.5.0 (Hadoop 3.3.4)
    hadoop_aws_package = "org.apache.hadoop:hadoop-aws:3.3.4"
    aws_sdk_package = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    packages = f"{hadoop_aws_package},{aws_sdk_package}"

    spark = SparkSession.builder \
        .appName("S3 Local Pipeline") \
        .master("local[*]") \
        .config("spark.jars.packages", packages) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .getOrCreate()
        
    return spark

# -------------------------------------------------------------------------
# 3. Main Execution Flow
# -------------------------------------------------------------------------
def main():
    print("--- Starting Local S3 PySpark Pipeline ---")
    
    # Check for credentials
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        print("ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables must be set.")
        sys.exit(1)

    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session Created Successfully.")

    input_path = get_s3_path(S3_INPUT_KEY)
    output_path = get_s3_path(S3_OUTPUT_KEY)
    
    print(f"\nAttempting to read from: {input_path}")
    
    try:
        # Try reading as CSV first (common format)
        # Adjust options (header, delimiter) as needed for your actual data
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        
        # If you are using Parquet, uncomment the line below:
        # df = spark.read.parquet(input_path)
        
        print("Data Loaded Successfully. Schema:")
        df.printSchema()
        
        print("Top 5 Rows:")
        df.show(5)
        
        # --- Simple Transformation ---
        print("Performing simple transformation (count)...")
        row_count = df.count()
        print(f"Total Rows: {row_count}")
        
        # --- Write Output ---
        print(f"\nWriting processed data to: {output_path}")
        
        # Write as Parquet (Recommended for performance)
        df.write.mode("overwrite").parquet(output_path)
        print("Successfully wrote Parquet output.")
        
        # Write as CSV (Optional, for human readability)
        # df.write.mode("overwrite").option("header", "true").csv(output_path + "_csv")
        # print("Successfully wrote CSV output.")
        
    except Exception as e:
        print(f"\nSpark Operation Failed: {e}")
        print("Possible causes:")
        print("1. Incorrect S3 Bucket Name or Key.")
        print("2. Missing or invalid AWS Credentials.")
        print("3. Network issues connecting to S3.")
        print("4. 'data.csv' does not exist in the 'raw/' folder.")

    # Cleanup
    spark.stop()
    print("\n--- Pipeline Finished ---")

if __name__ == "__main__":
    main()
