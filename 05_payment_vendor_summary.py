# -------------------------------------------------------------------------
# Filename: 05_payment_vendor_summary.py
import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def create_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        spark.conf.set("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
    
    if os.environ.get("AWS_DEFAULT_REGION"):
        spark.conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{os.environ.get('AWS_DEFAULT_REGION')}.amazonaws.com")

    return spark

def main():
    spark = create_spark_session("05_Payment_Vendor_Summary")
    spark.sparkContext.setLogLevel("WARN")
    
    bucket_name = os.environ.get("S3_BUCKET_NAME", "indian-weather-project-leywin")
    input_path = f"s3a://{bucket_name}/processed/master/products/"
    output_path = f"s3a://{bucket_name}/processed/summary/payment_vendor_summary/"
    
    print(f"Input Path: {input_path}")
    print(f"Output Path: {output_path}")

    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading input path: {e}")
        sys.exit(1)

    required_columns = ['vendor_type', 'payment_methods', 'year', 'revenue_INR', 'units_sold', 'discount_pct']
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        print(f"ERROR: Missing required columns: {missing_columns}")
        sys.exit(1)

    initial_count = df.count()
    print(f"Initial Row Count: {initial_count}")

    # Aggregation
    # Group by vendor_type, payment_methods, year
    summary_df = df.groupBy("vendor_type", "payment_methods", "year") \
        .agg(
            F.sum("revenue_INR").alias("total_revenue"),
            F.sum("units_sold").alias("total_units"),
            F.avg("discount_pct").alias("avg_discount")
        )

    print("Schema of Output:")
    summary_df.printSchema()
    
    final_count = summary_df.count()
    print(f"Summary Row Count: {final_count}")

    print(f"Writing to {output_path} ...")
    summary_df.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print("Success! Job finished.")
    spark.stop()

if __name__ == "__main__":
    main()
