import streamlit as st
import pandas as pd
import boto3
import time
import plotly.express as px
from ssm_manager import SSMManager
import io

# Page Config
st.set_page_config(page_title="PySpark Pipeline Dashboard", layout="wide")

# Sidebar Configuration
st.sidebar.title("Configuration")

# Credentials - Try to load from secrets or env, else input
# For Streamlit Cloud, these should be in st.secrets
if 'AWS_ACCESS_KEY_ID' in st.secrets:
    aws_access_key = st.secrets['AWS_ACCESS_KEY_ID']
    aws_secret_key = st.secrets['AWS_SECRET_ACCESS_KEY']
    region = st.secrets.get('AWS_DEFAULT_REGION', 'ap-south-1')
    bucket_name = st.secrets.get('S3_BUCKET_NAME', 'indian-weather-project-leywin')
    instance_id = st.secrets.get('EC2_INSTANCE_ID', '')
    
    # Show configured values (read-only)
    st.sidebar.success("✅ Credentials loaded from secrets")
    st.sidebar.text(f"Region: {region}")
    st.sidebar.text(f"Bucket: {bucket_name}")
    st.sidebar.text(f"Instance: {instance_id}")
else:
    aws_access_key = st.sidebar.text_input("AWS Access Key ID", type="password")
    aws_secret_key = st.sidebar.text_input("AWS Secret Access Key", type="password")
    region = st.sidebar.text_input("AWS Region", value="ap-south-1")
    bucket_name = st.sidebar.text_input("S3 Bucket Name", value="indian-weather-project-leywin")
    instance_id = st.sidebar.text_input("EC2 Instance ID", value="")

if not aws_access_key or not aws_secret_key or not instance_id:
    st.warning("Please provide AWS Credentials and Instance ID in the sidebar.")
    st.stop()

# Initialize SSM Manager
ssm_manager = SSMManager(aws_access_key, aws_secret_key, region)

# Initialize S3 Client for Data Preview
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region
)

st.title("PySpark Pipeline Dashboard 🚀")

# Tabs
tab1, tab2, tab3 = st.tabs(["Control Panel", "Data Preview", "Visualizations"])

# --- Tab 1: Control Panel ---
with tab1:
    st.header("Pipeline Control")
    
    scripts = [
        "01_overall_kpis.py",
        "02_festival_category_summary.py",
        "03_geo_summary.py",
        "04_weather_sales_summary.py",
        "05_payment_vendor_summary.py"
    ]
    
    selected_script = st.selectbox("Select Script to Run", scripts)
    
    if st.button("Run Pipeline Job"):
        with st.spinner(f"Triggering {selected_script} on {instance_id}..."):
            command_id, log_filename = ssm_manager.run_spark_job(instance_id, selected_script, bucket_name)
            
            if command_id:
                st.success(f"Job triggered! Command ID: {command_id}")
                st.session_state['current_command_id'] = command_id
                st.session_state['current_log_file'] = log_filename
                st.session_state['job_status'] = "InProgress"
            
    # Status Tracking
    if 'current_command_id' in st.session_state:
        st.divider()
        st.subheader("Job Status")
        
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"Command ID: {st.session_state['current_command_id']}")
        with col2:
            status_placeholder = st.empty()
            status_placeholder.text(f"Status: {st.session_state.get('job_status', 'Unknown')}")
            
        logs_placeholder = st.empty()
        
        # Poll for status if in progress
        if st.session_state.get('job_status') in ["InProgress", "Pending"]:
            if st.button("Refresh Status"):
                status, stdout, stderr = ssm_manager.get_job_status(st.session_state['current_command_id'], instance_id)
                st.session_state['job_status'] = status
                status_placeholder.text(f"Status: {status}")
                
                if status == "Success":
                    st.success("Job Completed Successfully!")
                    # Show SSM output
                    if stdout:
                        st.subheader("Output:")
                        logs_placeholder.code(stdout, language="bash")
                elif status == "Failed":
                    st.error("Job Failed!")
                    if stderr:
                        st.code(stderr, language="bash")
                    if stdout:
                        st.subheader("Output before failure:")
                        st.code(stdout, language="bash")
                else:
                    st.info("Job is running... Check back in a moment.")

# --- Tab 2: Data Preview ---
with tab2:
    st.header("S3 Data Preview")
    
    prefix = st.text_input("S3 Prefix (Folder)", value="processed/summary/")
    
    if st.button("List Files"):
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet') or obj['Key'].endswith('.csv')]
                st.session_state['file_list'] = files
            else:
                st.warning("No files found.")
        except Exception as e:
            st.error(f"Error listing files: {e}")
            
    if 'file_list' in st.session_state:
        selected_file = st.selectbox("Select File", st.session_state['file_list'])
        
        if st.button("Load Data"):
            try:
                obj = s3_client.get_object(Bucket=bucket_name, Key=selected_file)
                if selected_file.endswith('.csv'):
                    df = pd.read_csv(obj['Body'])
                elif selected_file.endswith('.parquet'):
                    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
                
                st.dataframe(df.head(500))
                st.session_state['preview_df'] = df
            except Exception as e:
                st.error(f"Error loading file: {e}")

# --- Tab 3: Visualizations ---
with tab3:
    st.header("Visualizations")
    
    if 'preview_df' in st.session_state:
        df = st.session_state['preview_df']
        st.write("Visualizing loaded data...")
        
        # Auto-detect columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
        
        if numeric_cols:
            st.subheader("Distribution")
            col_dist = st.selectbox("Select Numeric Column", numeric_cols)
            fig_hist = px.histogram(df, x=col_dist, title=f"Distribution of {col_dist}")
            st.plotly_chart(fig_hist)
            
        if numeric_cols and categorical_cols:
            st.subheader("Bar Chart")
            x_col = st.selectbox("X Axis (Categorical)", categorical_cols)
            y_col = st.selectbox("Y Axis (Numeric)", numeric_cols)
            fig_bar = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}")
            st.plotly_chart(fig_bar)
            
    else:
        st.info("Please load a dataset in the 'Data Preview' tab first.")
