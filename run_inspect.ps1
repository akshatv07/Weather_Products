# PowerShell Script to Run Local Data Inspection
# Automatically sets up Java, Hadoop, Python environment variables.

# 1. Set Java 17 Path
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.9.8-hotspot"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"

# 2. Set Hadoop Home (Winutils)
$env:HADOOP_HOME = "C:\Users\Akshat\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"

# 3. Set Python Path (Anaconda)
$env:PYSPARK_PYTHON = "C:\Users\Akshat\anaconda3\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Users\Akshat\anaconda3\python.exe"

# 4. Run Spark Submit
Write-Host "Starting Local Data Inspection..."
# We don't need AWS packages for local files
spark-submit inspect_data.py > inspect_output.txt 2>&1
