import pandas as pd
import os

def convert_excel_to_csv():
    input_file = "indian_festival_products_200k.xlsx"
    output_file = "indian_festival_products_200k.csv"
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.")
        return

    print(f"Reading {input_file}...")
    try:
        df = pd.read_excel(input_file)
        print(f"Converting to {output_file}...")
        df.to_csv(output_file, index=False)
        print("Conversion successful!")
    except Exception as e:
        print(f"Error converting file: {e}")

if __name__ == "__main__":
    convert_excel_to_csv()
