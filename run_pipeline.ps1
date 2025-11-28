# PowerShell Script to Run S3 PySpark Pipeline
# Automatically sets up Java, Hadoop, Python, and AWS environment variables.

# 1. Set Java 17 Path
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.9.8-hotspot"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"

# 2. Set Hadoop Home (Winutils)
$env:HADOOP_HOME = "C:\Users\Akshat\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"

# 3. Set Python Path (Anaconda)
$env:PYSPARK_PYTHON = "C:\Users\Akshat\anaconda3\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Users\Akshat\anaconda3\python.exe"

# 4. Set AWS Credentials
# Credentials should be set in the session or via profile
# $env:AWS_ACCESS_KEY_ID = "YOUR_ACCESS_KEY"
# $env:AWS_SECRET_ACCESS_KEY = "YOUR_SECRET_KEY"
$env:AWS_DEFAULT_REGION = "ap-south-1"

# 5. Run Spark Submit
Write-Host "Starting PySpark Pipeline..."
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 s3_pyspark_pipeline.py
