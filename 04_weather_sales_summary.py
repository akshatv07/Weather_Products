# -------------------------------------------------------------------------
# Filename: 04_weather_sales_summary.py
# Purpose: Correlate weather buckets with sales.
# Input: 
import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType

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
    spark = create_spark_session("04_Weather_Sales_Summary")
    spark.sparkContext.setLogLevel("WARN")
    
    bucket_name = os.environ.get("S3_BUCKET_NAME", "indian-weather-project-leywin")
    products_path = f"s3a://{bucket_name}/processed/master/products/"
    weather_path = f"s3a://{bucket_name}/processed/master/weather_daily/"
    output_path = f"s3a://{bucket_name}/processed/summary/weather_sales_summary/"
    
    print(f"Products Path: {products_path}")
    print(f"Weather Path: {weather_path}")
    print(f"Output Path: {output_path}")

    # Read Inputs
    try:
        products_df = spark.read.parquet(products_path)
        weather_df = spark.read.parquet(weather_path)
    except Exception as e:
        print(f"Error reading input paths: {e}")
        sys.exit(1)

    # Validation
    prod_req = ['city', 'festival_date', 'revenue_INR', 'units_sold', 'price_INR']
    weath_req = ['city', 'date', 'avg_temp_C', 'avg_humidity_pct', 'total_precip_mm']
    
    if any(c not in products_df.columns for c in prod_req):
        print(f"ERROR: Missing columns in Products. Required: {prod_req}")
        sys.exit(1)
    if any(c not in weather_df.columns for c in weath_req):
        print(f"ERROR: Missing columns in Weather. Required: {weath_req}")
        sys.exit(1)

    print(f"Products Count: {products_df.count()}")
    print(f"Weather Count: {weather_df.count()}")

    # Prepare Join Keys
    # Ensure festival_date aligns with weather date.
    products_df = products_df.withColumn("join_date", F.col("festival_date").cast(DateType()))
    weather_df = weather_df.withColumn("join_date", F.col("date").cast(DateType()))

    # Join
    # Left join products with weather
    joined_df = products_df.join(weather_df, on=["city", "join_date"], how="left")

    # Check for unmatched rows
    unmatched_count = joined_df.filter(F.col("avg_temp_C").isNull()).count()
    total_count = joined_df.count()
    
    print(f"Joined Rows: {total_count}")
    print(f"Unmatched Rows (No Weather Data): {unmatched_count}")
    
    if unmatched_count > 0 and (unmatched_count / total_count) > 0.05:
        print("WARNING: >5% of sales rows have no matching weather data.")
        debug_path = f"s3a://{bucket_name}/processed/debug/unmatched_joins/"
        print(f"Writing sample unmatched rows to {debug_path}")
        joined_df.filter(F.col("avg_temp_C").isNull()) \
            .select("city", "join_date") \
            .limit(10) \
            .write.mode("overwrite").csv(debug_path)

    # Bucketing Logic
    # Temp: <15, 15-25, 25-35, >35
    # Humidity: <40, 40-70, >70
    # Rain: >0
    
    def get_temp_bucket(t):
        return F.when(t < 15, "<15") \
                .when((t >= 15) & (t <= 25), "15-25") \
                .when((t > 25) & (t <= 35), "25-35") \
                .otherwise(">35")

    def get_humid_bucket(h):
        return F.when(h < 40, "<40") \
                .when((h >= 40) & (h <= 70), "40-70") \
                .otherwise(">70")

    processed_df = joined_df.withColumn("temp_bucket", get_temp_bucket(F.col("avg_temp_C"))) \
                            .withColumn("humidity_bucket", get_humid_bucket(F.col("avg_humidity_pct"))) \
                            .withColumn("rain_flag", F.when(F.col("total_precip_mm") > 0, True).otherwise(False)) \
                            .withColumn("year", F.year("join_date")) \
                            .withColumn("month", F.month("join_date"))

    # Aggregation
    summary_df = processed_df.groupBy("city", "join_date", "year", "month", "temp_bucket", "humidity_bucket", "rain_flag") \
        .agg(
            F.sum("revenue_INR").alias("total_revenue"),
            F.sum("units_sold").alias("total_units"),
            F.avg("price_INR").alias("avg_price")
        ) \
        .withColumnRenamed("join_date", "date")

    print("Schema of Output:")
    summary_df.printSchema()
    
    print(f"Writing to {output_path} ...")
    summary_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)
    
    print("Success! Job finished.")
    spark.stop()

if __name__ == "__main__":
    main()
