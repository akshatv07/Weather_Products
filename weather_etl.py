# -------------------------------------------------------------------------
# Filename: weather_etl.py
# Purpose: Transform S3 Raw Weather CSVs into Master Parquet.
# Run on EC2: spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 weather_etl.py
# -------------------------------------------------------------------------

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
    
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        spark.conf.set("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
    
    if os.environ.get("AWS_DEFAULT_REGION"):
        spark.conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{os.environ.get('AWS_DEFAULT_REGION')}.amazonaws.com")

    return spark

def process_weather(spark, bucket_name):
    print("\n--- Processing Weather ---")
    input_path = f"s3a://{bucket_name}/raw/*.csv"
    output_path = f"s3a://{bucket_name}/processed/master/weather_daily/"
    
    print(f"Reading from: {input_path}")
    
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        
        df = df.withColumn("filename", F.input_file_name())
        # Extract city from filename (s3a://.../raw/CityName.csv)
        df = df.withColumn("city", F.regexp_extract("filename", r"([^/]+)\.csv$", 1))
        
        # Cast date
        df = df.withColumn("timestamp", F.to_timestamp("date")) \
               .withColumn("date_only", F.to_date("timestamp"))
        
        # Aggregate to Daily
        daily_df = df.groupBy("city", "date_only") \
            .agg(
                F.avg("temperature_2m").alias("avg_temp_C"),
                F.avg("relative_humidity_2m").alias("avg_humidity_pct"),
                F.sum("precipitation").alias("total_precip_mm")
            )
            
        # Add derived columns
        daily_df = daily_df.withColumnRenamed("date_only", "date") \
                           .withColumn("rain_flag", F.when(F.col("total_precip_mm") > 0, True).otherwise(False)) \
                           .withColumn("year", F.year("date")) \
                           .withColumn("month", F.month("date")) \
                           .withColumn("day", F.dayofmonth("date")) \
                           .withColumn("state", F.lit("Unknown")) \
                           .withColumn("latitude", F.lit(0.0)) \
                           .withColumn("longitude", F.lit(0.0))
                           
        print(f"Writing to {output_path}")
        daily_df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
        print("Success!")
        
    except Exception as e:
        print(f"Error processing Weather: {e}")

def main():
    spark = create_spark_session("Weather_ETL")
    spark.sparkContext.setLogLevel("WARN")
    
    bucket_name = os.environ.get("S3_BUCKET_NAME", "indian-weather-project-leywin")
    process_weather(spark, bucket_name)
    
    spark.stop()

if __name__ == "__main__":
    main()
