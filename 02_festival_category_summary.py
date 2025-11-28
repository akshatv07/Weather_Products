# -------------------------------------------------------------------------
# Filename: 02_festival_category_summary.py
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
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
    spark = create_spark_session("02_Festival_Category_Summary")
    spark.sparkContext.setLogLevel("WARN")
    
    bucket_name = os.environ.get("S3_BUCKET_NAME", "indian-weather-project-leywin")
    input_path = f"s3a://{bucket_name}/processed/master/products/"
    output_path = f"s3a://{bucket_name}/processed/summary/festival_category_summary/"
    
    print(f"Input Path: {input_path}")
    print(f"Output Path: {output_path}")

    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading input path: {e}")
        sys.exit(1)

    required_columns = ['festival', 'product_category', 'year', 'revenue_INR', 'units_sold', 'price_INR', 'product']
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        print(f"ERROR: Missing required columns: {missing_columns}")
        sys.exit(1)

    initial_count = df.count()
    print(f"Initial Row Count: {initial_count}")

    # 1. Base Aggregation: Festival x Category x Year
    base_agg = df.groupBy("festival", "product_category", "year") \
        .agg(
            F.sum("revenue_INR").alias("total_revenue"),
            F.sum("units_sold").alias("total_units"),
            F.avg("price_INR").alias("avg_price")
        )

    # 2. Top Products per Festival
    # We need to aggregate revenue by product per festival first
    product_agg = df.groupBy("festival", "year", "product") \
        .agg(F.sum("revenue_INR").alias("prod_revenue"))
    
    window_spec = Window.partitionBy("festival", "year").orderBy(F.col("prod_revenue").desc())
    
    top_products_df = product_agg.withColumn("rank", F.rank().over(window_spec)) \
        .filter(F.col("rank") <= 5) \
        .groupBy("festival", "year") \
        .agg(
            F.collect_list(
                F.struct(F.col("product"), F.col("prod_revenue").alias("revenue"))
            ).alias("top_products")
        )

    # 3. Join Base Aggregation with Top Products
    # Note: Top products are at Festival level, Base is at Festival x Category level.
    # The requirement asks to attach top products to festival rows. 
    # Since the base granularity is category, the top products (which are festival-wide) will be repeated for each category in that festival.
    
    final_df = base_agg.join(top_products_df, on=["festival", "year"], how="left")

    print("Schema of Output:")
    final_df.printSchema()
    
    final_count = final_df.count()
    print(f"Summary Row Count: {final_count}")

    print(f"Writing to {output_path} ...")
    final_df.write \
        .mode("overwrite") \
        .partitionBy("year") \
        .parquet(output_path)
    
    print("Success! Job finished.")
    spark.stop()

if __name__ == "__main__":
    main()
