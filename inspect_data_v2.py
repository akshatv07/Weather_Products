import os
import sys
import pandas as pd
from pyspark.sql import SparkSession

OUTPUT_FILE = "inspection_results.txt"

def log(message):
    print(message)
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(str(message) + "\n")

def create_spark_session():
    return SparkSession.builder \
        .appName("Local Data Inspector") \
        .master("local[*]") \
        .getOrCreate()

def inspect_csv_spark(spark, file_path, name):
    log(f"\n{'='*80}")
    log(f"INSPECTING (Spark): {name}")
    log(f"Path: {file_path}")
    log(f"{'='*80}")
    
    if not os.path.exists(file_path):
        log(f"ERROR: File not found at {file_path}")
        return

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        
        log("\n--- COLUMNS ---")
        log(df.columns)
        
        log("\n--- FIRST 3 ROWS ---")
        # Capture show() output
        rows = df.head(3)
        for row in rows:
            log(row)
        
    except Exception as e:
        log(f"\nERROR reading {name}: {e}")

def inspect_excel_pandas(file_path, name):
    log(f"\n{'='*80}")
    log(f"INSPECTING (Pandas): {name}")
    log(f"Path: {file_path}")
    log(f"{'='*80}")

    if not os.path.exists(file_path):
        log(f"ERROR: File not found at {file_path}")
        return

    try:
        df = pd.read_excel(file_path, nrows=3)
        
        log("\n--- COLUMNS ---")
        log(list(df.columns))
        
        log("\n--- FIRST 3 ROWS ---")
        log(df.head(3).to_string())
        
    except Exception as e:
        log(f"\nERROR reading {name}: {e}")

def main():
    # Clear previous file
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    log("--- Starting Local Data Inspection ---")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    log("Spark Session Created Successfully.\n")

    # 1. Inspect Festival_Sales.csv
    inspect_csv_spark(spark, "Festival_Sales.csv", "Festival Sales")

    # 2. Inspect Abbigeri.csv
    inspect_csv_spark(spark, "Cities/Abbigeri.csv", "Abbigeri Data")

    # 3. Inspect indian_festival_products_200k.xlsx
    inspect_excel_pandas("indian_festival_products_200k.xlsx", "Indian Festival Products")

    spark.stop()
    log("\n--- Inspection Finished ---")

if __name__ == "__main__":
    main()
