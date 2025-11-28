# -------------------------------------------------------------------------
# Filename: 01_overall_kpis.py
import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType

def create_spark_session(app_name):
    """
    Creates a SparkSession configured for S3 access on EC2.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    # Set credentials from environment variables
    # Recommended: Use IAM Role on EC2 instead of hardcoded keys.
    # If using IAM Role, comment out the following lines.
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        spark.conf.set("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
    
    if os.environ.get("AWS_DEFAULT_REGION"):
        spark.conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{os.environ.get('AWS_DEFAULT_REGION')}.amazonaws.com")

    return spark

def main():
    # 1. Setup
    spark = create_spark_session("01_Overall_KPIs")
    spark.sparkContext.setLogLevel("WARN")
    
    bucket_name = os.environ.get("S3_BUCKET_NAME", "indian-weather-project-leywin")
    input_path = f"s3a://{bucket_name}/processed/master/products/"
    output_path = f"s3a://{bucket_name}/processed/summary/overall_kpis/"
    
    print(f"Input Path: {input_path}")
    print(f"Output Path: {output_path}")

    # 2. Read Input
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading input path: {e}")
        sys.exit(1)

    # 3. Validation
    required_columns = ['festival_date', 'revenue_INR', 'units_sold', 'price_INR', 'discount_pct', 'seller_rating']
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        print(f"ERROR: Missing required columns: {missing_columns}")
        sys.exit(1)

    initial_count = df.count()
    print(f"Initial Row Count: {initial_count}")

    # 4. Aggregation
    # Group by festival_date as date -> compute sums/avgs
    summary_df = df.groupBy(F.col("festival_date").alias("date")) \
        .agg(
            F.sum("revenue_INR").alias("total_revenue"),
            F.sum("units_sold").alias("total_units"),
            F.count("*").alias("total_tx"),
            F.avg("price_INR").alias("avg_price"),
            F.avg("discount_pct").alias("avg_discount"),
            F.avg("seller_rating").alias("avg_seller_rating")
        )

    # Add year/month/day columns for partitioning and querying
    summary_df = summary_df.withColumn("date", F.col("date").cast(DateType())) \
                           .withColumn("year", F.year("date")) \
                           .withColumn("month", F.month("date")) \
                           .withColumn("day", F.dayofmonth("date"))

    # 5. Output
    print("Schema of Output:")
    summary_df.printSchema()
    
    final_count = summary_df.count()
    print(f"Summary Row Count: {final_count}")

    print(f"Writing to {output_path} ...")
    summary_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)
    
    print("Success! Job finished.")
    spark.stop()

if __name__ == "__main__":
    main()
