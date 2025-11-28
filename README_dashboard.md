# Serverless PySpark Dashboard Deployment Guide 🚀

This guide explains how to deploy the PySpark Dashboard on **Streamlit Community Cloud** (Free) and connect it to your AWS EC2 instance using **AWS Systems Manager (SSM)**.

## Prerequisites
- An AWS Account.
- An EC2 instance running your PySpark environment.
- A GitHub account (to host the dashboard code).

## Step 1: Configure AWS EC2 for SSM
Your EC2 instance needs permission to accept commands from SSM.

1.  **Log in to AWS Console** and go to **IAM**.
2.  **Create a Role** for EC2:
    - Select **AWS Service** -> **EC2**.
    - Search for and attach the policy: `AmazonSSMManagedInstanceCore`.
    - Name the role: `EC2SSMRole`.
3.  **Attach Role to EC2**:
    - Go to **EC2 Console** -> Select your instance.
    - **Actions** -> **Security** -> **Modify IAM role**.
    - Select `EC2SSMRole` and click **Update IAM role**.
4.  **Verify SSM Agent**:
    - SSH into your instance and run: `sudo systemctl status snap.amazon-ssm-agent.amazon-ssm-agent.service`
    - It should be **Active (running)**.

## Step 2: Create an IAM User for the Dashboard
The Streamlit app needs an AWS user with specific permissions to trigger commands and read S3.

1.  Go to **IAM Console** -> **Users** -> **Create user**.
2.  Name it `streamlit-dashboard-user`.
3.  **Attach policies directly**:
    - Click **Create policy** -> **JSON**.
    - Paste the content of `infra/iam_policy.json`.
    - Name it `StreamlitDashboardPolicy` and create it.
4.  **Create Access Keys**:
    - Go to the user's **Security credentials** tab.
    - Create an **Access key**.
    - **Save the Access Key ID and Secret Access Key**. You will need these later.

## Step 3: Deploy to Streamlit Cloud
1.  **Push your code to GitHub**:
    - Ensure `dashboard/app.py`, `dashboard/ssm_manager.py`, and `dashboard/requirements.txt` are in your repo.
2.  **Sign up/Login** to [share.streamlit.io](https://share.streamlit.io/).
3.  Click **New app**.
4.  Select your Repository, Branch, and file path (`dashboard/app.py`).
5.  **Advanced Settings (Secrets)**:
    - Paste the following TOML config into the secrets area:
    ```toml
    AWS_ACCESS_KEY_ID = "YOUR_ACCESS_KEY_ID"
    AWS_SECRET_ACCESS_KEY = "YOUR_SECRET_ACCESS_KEY"
    AWS_DEFAULT_REGION = "ap-south-1"
    S3_BUCKET_NAME = "indian-weather-project-leywin"
    EC2_INSTANCE_ID = "i-0xxxxxxxxxxxx"
    ```
    - Replace the values with your actual credentials and Instance ID.
6.  Click **Deploy!** 🎈

## Step 4: Using the Dashboard
- **Control Panel**: Select a script (e.g., `01_overall_kpis.py`) and click **Run Pipeline Job**.
- **Status**: Watch the status update from `Pending` -> `InProgress` -> `Success`.
- **Logs**: Once finished, the logs will appear automatically.
- **Data Preview**: Go to the "Data Preview" tab to explore the generated Parquet files in S3.
