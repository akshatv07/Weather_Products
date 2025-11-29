import boto3
import time
import streamlit as st

class SSMManager:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.ssm = boto3.client(
            'ssm',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def run_spark_job(self, instance_id, script_name, bucket_name):
        """
        Triggers a Spark job on the EC2 instance via SSM.
        Captures output via SSM (no S3 upload needed).
        """
        # Construct the command
        log_filename = f"{script_name.replace('.py', '')}_{int(time.time())}.log"
        
        # Run spark-submit and capture output
        # Note: We don't upload to S3 since AWS CLI may not be installed
        # SSM will capture stdout/stderr for us
        
        commands = [
            f"cd /home/ubuntu",
            f"export PATH=$PATH:/home/ubuntu/.local/bin:/usr/lib/spark/bin",
            f"spark-submit --master local[*] --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 {script_name} 2>&1"
        ]
        
        try:
            response = self.ssm.send_command(
                InstanceIds=[instance_id],
                DocumentName="AWS-RunShellScript",
                Parameters={'commands': commands},
                Comment=f"Run Spark Job: {script_name}"
            )
            return response['Command']['CommandId'], log_filename
        except Exception as e:
            st.error(f"Failed to trigger job: {str(e)}")
            return None, None

    def get_job_status(self, command_id, instance_id):
        """
        Checks the status of the SSM command.
        """
        try:
            response = self.ssm.get_command_invocation(
                CommandId=command_id,
                InstanceId=instance_id
            )
            return response['Status'], response['StandardOutputContent'], response['StandardErrorContent']
        except Exception as e:
            return "Error", str(e), ""

    def read_log_from_s3(self, bucket_name, log_filename):
        """
        Reads the log file from S3.
        """
        try:
            obj = self.s3.get_object(Bucket=bucket_name, Key=f"logs/{log_filename}")
            return obj['Body'].read().decode('utf-8')
        except Exception as e:
            return f"Log not found yet or error reading: {str(e)}"
