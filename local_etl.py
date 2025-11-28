# -------------------------------------------------------------------------
# Filename: local_etl.py
# Purpose: Transform local CSVs into Master Parquet datasets locally.
# Run: spark-submit local_etl.py
# -------------------------------------------------------------------------

import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def create_local_spark_session():
    return SparkSession.builder \
        .appName("Local_ETL") \
        .master("local[*]") \
        .getOrCreate()

def process_festival_sales(spark):
    print("\n--- Processing Festival Sales ---")
    input_path = "Festival_Sales.csv"
    output_path = "processed_local/festival_sales/"
    
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        
        # Normalize columns
        # Expected: ['ProductID','ProductName','Category','Sales','State','Region','City','Latitude','Longitude','Date','Festival','date']
        # Input Date format is likely dd-MM-yyyy based on inspection (03-11-2015)
        
        df = df.withColumn("date", F.to_date(F.col("Date"), "dd-MM-yyyy"))
        
        print(f"Writing to {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        print("Success!")
    except Exception as e:
        print(f"Error processing Festival Sales: {e}")

def process_products(spark):
    print("\n--- Processing Products ---")
    input_path = "indian_festival_products_200k.csv"
    output_path = "processed_local/products/"
    
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found. Please run local_excel_to_csv.py first.")
        return

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
        print("Success!")
    except Exception as e:
        print(f"Error processing Products: {e}")

def process_weather(spark):
    print("\n--- Processing Weather (Local) ---")
    cities_folder = "Cities"
    city_list_file = "uploaded_city_names.csv"
    output_path = "processed_local/weather_daily/"
    
    if not os.path.exists(city_list_file):
        print(f"Error: {city_list_file} not found.")
        return

    # Read city names
    try:
        with open(city_list_file, 'r') as f:
            # Skip header if exists, assuming first line is header 'City Name'
            lines = f.readlines()
            cities = [line.strip() for line in lines[1:] if line.strip()]
            print(f"Found {len(cities)} cities in {city_list_file}")
    except Exception as e:
        print(f"Error reading city list: {e}")
        return

    # Construct file paths
    valid_files = []
    for city in cities:
        # Handle potential spaces in filenames if they exist in the folder
        # The list has 'Akbarpur 2', folder has 'Akbarpur_2.csv' or 'Akbarpur 2.csv'?
        # Inspection of list_dir showed 'Akbarpur_2.csv'. 
        # The CSV list has 'Akbarpur 2'. We need to replace spaces with underscores?
        # Let's check a few examples.
        # CSV: 'Akbarpur 2', 'Alampur 2'
        # Dir: 'Akbarpur_2.csv', 'Alampur_2.csv'
        # So we likely need to replace spaces with underscores for the filename.
        
        filename = city.replace(" ", "_") + ".csv"
        file_path = os.path.join(cities_folder, filename)
        
        if os.path.exists(file_path):
            valid_files.append(file_path)
        else:
            # Try exact name just in case
            file_path_exact = os.path.join(cities_folder, city + ".csv")
            if os.path.exists(file_path_exact):
                valid_files.append(file_path_exact)
    
    print(f"Found {len(valid_files)} valid weather CSV files.")
    
    if not valid_files:
        print("No valid weather files found to process.")
        return

    try:
        # Read all files at once
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(valid_files)
        
        df = df.withColumn("filename", F.input_file_name())
        # Extract city from filename. 
        # Windows path might have backslashes, F.input_file_name() returns URI usually (file:///...) or forward slashes.
        # Regex to capture name before .csv
        df = df.withColumn("city", F.regexp_extract("filename", r"([^/|\\]+)\.csv$", 1))
        
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
    spark = create_local_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    process_festival_sales(spark)
    process_products(spark)
    process_weather(spark)
    
    spark.stop()
    print("\nLocal ETL Finished. Please upload the 'processed_local' folder to S3 as 'processed/master'.")

if __name__ == "__main__":
    main()
