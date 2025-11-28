import os
import sys
import pandas as pd
from pyspark.sql import SparkSession

def create_spark_session():
    """
    Creates a local SparkSession.
    """
    return SparkSession.builder \
        .appName("Local Data Inspector") \
        .master("local[*]") \
        .getOrCreate()

def inspect_csv_spark(spark, file_path, name):
    print(f"\n{'='*80}")
    print(f"INSPECTING (Spark): {name}")
    print(f"Path: {file_path}")
    print(f"{'='*80}")
    
    if not os.path.exists(file_path):
        print(f"ERROR: File not found at {file_path}")
        return

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        
        print("\n--- COLUMNS ---")
        print(df.columns)
        
        print("\n--- FIRST 3 ROWS ---")
        df.show(3, truncate=False)
        
    except Exception as e:
        print(f"\nERROR reading {name}: {e}")

def inspect_excel_pandas(file_path, name):
    print(f"\n{'='*80}")
    print(f"INSPECTING (Pandas): {name}")
    print(f"Path: {file_path}")
    print(f"{'='*80}")

    if not os.path.exists(file_path):
        print(f"ERROR: File not found at {file_path}")
        return

    try:
        # Read only first few rows for efficiency
        df = pd.read_excel(file_path, nrows=3)
        
        print("\n--- COLUMNS ---")
        print(list(df.columns))
        
        print("\n--- FIRST 3 ROWS ---")
        print(df.head(3).to_string())
        
    except Exception as e:
        print(f"\nERROR reading {name}: {e}")

def main():
    print("--- Starting Local Data Inspection ---")
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR") # Reduce log noise
    print("Spark Session Created Successfully.\n")

    # 1. Inspect Festival_Sales.csv
    inspect_csv_spark(spark, "Festival_Sales.csv", "Festival Sales")

    # 2. Inspect Abbigeri.csv
    inspect_csv_spark(spark, "Cities/Abbigeri.csv", "Abbigeri Data")

    # 3. Inspect indian_festival_products_200k.xlsx
    inspect_excel_pandas("indian_festival_products_200k.xlsx", "Indian Festival Products")

    # Cleanup
    spark.stop()
    print("\n--- Inspection Finished ---")

if __name__ == "__main__":
    main()
