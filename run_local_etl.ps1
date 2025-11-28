# Run Local ETL
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.9.8-hotspot"
$env:HADOOP_HOME = "C:\Users\Akshat\hadoop"
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
$env:PYSPARK_PYTHON = "C:\Users\Akshat\anaconda3\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Users\Akshat\anaconda3\python.exe"

Write-Host "1. Converting Excel to CSV..."
python local_excel_to_csv.py

Write-Host "2. Running Spark ETL..."
spark-submit local_etl.py
