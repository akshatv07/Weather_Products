# -------------------------------------------------------------------------
# Filename: 00_etl_master_data.py
# Purpose: Transform raw CSVs into Master Parquet datasets.
#
# EC2 Run Command:
# spark-submit --master local[*] --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 00_etl_master_data.py
# -------------------------------------------------------------------------

import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, LongType

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

def process_festival_sales(spark, bucket_name):
    print("\n--- Processing Festival Sales ---")
    input_path = f"s3a://{bucket_name}/Festival_Sales.csv"
    output_path = f"s3a://{bucket_name}/processed/master/festival_sales/"
    
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        
        # Normalize columns
        # Expected: ['ProductID','ProductName','Category','Sales','State','Region','City','Latitude','Longitude','Date','Festival','date']
        # Input Date format is likely dd-MM-yyyy based on inspection (03-11-2015)
        
        df = df.withColumn("date", F.to_date(F.col("Date"), "dd-MM-yyyy"))
        
        print(f"Writing to {output_path}")
        df.write.mode("overwrite").parquet(output_path)
    except Exception as e:
        print(f"Error processing Festival Sales: {e}")

def process_products(spark, bucket_name):
    print("\n--- Processing Products ---")
    # Assumes user uploaded the CSV version
    input_path = f"s3a://{bucket_name}/indian_festival_products_200k.csv"
    output_path = f"s3a://{bucket_name}/processed/master/products/"
    
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        
        # Expected: ['product_id', ..., 'festival_date', ..., 'year', 'month', 'day']
        # Input festival_date format: 21-08-2025 (dd-MM-yyyy)
        
        df = df.withColumn("festival_date", F.to_date(F.col("festival_date"), "dd-MM-yyyy")) \
               .withColumn("year", F.year("festival_date")) \
               .withColumn("month", F.month("festival_date")) \
               .withColumn("day", F.dayofmonth("festival_date"))
        
        print(f"Writing to {output_path}")
        df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
    except Exception as e:
        print(f"Error processing Products: {e}")

def process_weather(spark, bucket_name):
    print("\n--- Processing Weather ---")
    input_path = f"s3a://{bucket_name}/raw/*.csv"
    output_path = f"s3a://{bucket_name}/processed/master/weather_daily/"
    
    try:
        # Read all CSVs in raw/
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        
        # Add City column from filename if possible, or assume it's not needed if we join by lat/lon?
        # Wait, the requirement says join by 'city'.
        # The raw files are named 'Abbigeri.csv'. Spark's input_file_name() can capture this.
        
        df = df.withColumn("filename", F.input_file_name())
        # Extract city from filename (s3a://.../raw/CityName.csv)
        # We'll split by '/' and take the last part, then remove .csv
        
        df = df.withColumn("city", F.regexp_extract("filename", r"([^/]+)\.csv$", 1))
        
        # Columns mapping based on inspection:
        # date (datetime string), temperature_2m, relative_humidity_2m, precipitation
        # Target: ['city','state','date','avg_temp_C','avg_humidity_pct','total_precip_mm','rain_flag','latitude','longitude','year','month','day']
        
        # Note: Raw data seems to be hourly (date has time). We need to aggregate to daily.
        
        # 1. Cast date
        df = df.withColumn("timestamp", F.to_timestamp("date")) \
               .withColumn("date_only", F.to_date("timestamp"))
        
        # 2. Aggregate to Daily
        daily_df = df.groupBy("city", "date_only") \
            .agg(
                F.avg("temperature_2m").alias("avg_temp_C"),
                F.avg("relative_humidity_2m").alias("avg_humidity_pct"),
                F.sum("precipitation").alias("total_precip_mm")
            )
            
        # 3. Add derived columns
        daily_df = daily_df.withColumnRenamed("date_only", "date") \
                           .withColumn("rain_flag", F.when(F.col("total_precip_mm") > 0, True).otherwise(False)) \
                           .withColumn("year", F.year("date")) \
                           .withColumn("month", F.month("date")) \
                           .withColumn("day", F.dayofmonth("date")) \
                           .withColumn("state", F.lit("Unknown")) \
                           .withColumn("latitude", F.lit(0.0)) \
                           .withColumn("longitude", F.lit(0.0))
                           
        # Note: State/Lat/Lon are missing in raw weather files based on inspection. 
        # We will fill with dummy values for now as they come from the Products dataset in the join usually.
        
        print(f"Writing to {output_path}")
        daily_df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
        
    except Exception as e:
        print(f"Error processing Weather: {e}")

def main():
    spark = create_spark_session("00_ETL_Master_Data")
    spark.sparkContext.setLogLevel("WARN")
    
    bucket_name = os.environ.get("S3_BUCKET_NAME", "indian-weather-project-leywin")
    
    process_festival_sales(spark, bucket_name)
    process_products(spark, bucket_name)
    process_weather(spark, bucket_name)
    
    spark.stop()

if __name__ == "__main__":
    main()
