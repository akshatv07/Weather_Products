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
                
                # Handle nested columns (like top_products in festival_category_summary)
                if 'top_products' in df.columns:
                    # Convert nested struct to readable string
                    def format_top_products(products):
                        if products is None or (isinstance(products, float) and pd.isna(products)):
                            return "N/A"
                        try:
                            # products is a list of dicts like [{'product': 'X', 'revenue': 123}, ...]
                            if isinstance(products, list):
                                return ", ".join([f"{p['product']} (₹{p['revenue']:,.0f})" for p in products])
                            return str(products)
                        except:
                            return str(products)
                    
                    df['top_products'] = df['top_products'].apply(format_top_products)
                
                st.dataframe(df.head(500))
                st.session_state['preview_df'] = df
            except Exception as e:
                st.error(f"Error loading file: {e}")

# --- Tab 3: Visualizations ---
with tab3:
    st.header("📊 Data Visualizations")
    
    if 'preview_df' in st.session_state:
        df = st.session_state['preview_df']
        
        # Auto-detect columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
        
        # Metrics Cards
        if numeric_cols:
            st.subheader("📈 Key Metrics")
            cols = st.columns(min(4, len(numeric_cols)))
            for i, col in enumerate(numeric_cols[:4]):
                with cols[i]:
                    # Format value based on column type
                    if 'revenue' in col.lower():
                        value = f"₹{df[col].sum():,.0f}"
                    elif 'units' in col.lower() or 'products' in col.lower() or 'sold' in col.lower():
                        value = f"{df[col].sum():,.0f}"
                    else:
                        value = f"{df[col].mean():.2f}"
                    
                    st.metric(
                        label=col.replace('_', ' ').title(),
                        value=value,
                        delta=f"{df[col].std():.2f}" if len(df) > 1 else None
                    )
        
        st.divider()
        
        # Chart Type Selection
        chart_type = st.radio(
            "Select Chart Type",
            ["Bar Chart", "Pie Chart", "Line Chart", "Scatter Plot", "Heatmap"],
            horizontal=True
        )
        
        if chart_type == "Bar Chart" and numeric_cols and categorical_cols:
            col1, col2 = st.columns(2)
            with col1:
                x_col = st.selectbox("X Axis (Category)", categorical_cols, key="bar_x")
            with col2:
                y_col = st.selectbox("Y Axis (Value)", numeric_cols, key="bar_y")
            
            # Color by category
            color_col = st.selectbox("Color By", [None] + categorical_cols, key="bar_color")
            
            fig = px.bar(
                df, 
                x=x_col, 
                y=y_col, 
                color=color_col if color_col else x_col,
                title=f"{y_col.replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}",
                color_discrete_sequence=px.colors.qualitative.Set3,
                template="plotly_dark"
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            
        elif chart_type == "Pie Chart" and numeric_cols and categorical_cols:
            col1, col2 = st.columns(2)
            with col1:
                names_col = st.selectbox("Categories", categorical_cols, key="pie_names")
            with col2:
                values_col = st.selectbox("Values", numeric_cols, key="pie_values")
            
            # Aggregate data
            pie_data = df.groupby(names_col)[values_col].sum().reset_index()
            
            fig = px.pie(
                pie_data,
                names=names_col,
                values=values_col,
                title=f"{values_col.replace('_', ' ').title()} Distribution",
                color_discrete_sequence=px.colors.qualitative.Pastel,
                template="plotly_dark"
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            
        elif chart_type == "Line Chart" and numeric_cols:
            # Check if there's a date column
            date_cols = [col for col in df.columns if 'date' in col.lower() or 'year' in col.lower()]
            
            if date_cols:
                col1, col2 = st.columns(2)
                with col1:
                    x_col = st.selectbox("X Axis (Time)", date_cols, key="line_x")
                with col2:
                    y_col = st.selectbox("Y Axis (Value)", numeric_cols, key="line_y")
                
                color_col = st.selectbox("Group By", [None] + categorical_cols, key="line_color")
                
                fig = px.line(
                    df,
                    x=x_col,
                    y=y_col,
                    color=color_col,
                    title=f"{y_col.replace('_', ' ').title()} Over Time",
                    color_discrete_sequence=px.colors.qualitative.Bold,
                    template="plotly_dark"
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No date/time column found for line chart. Try another chart type.")
                
        elif chart_type == "Scatter Plot" and len(numeric_cols) >= 2:
            col1, col2, col3 = st.columns(3)
            with col1:
                x_col = st.selectbox("X Axis", numeric_cols, key="scatter_x")
            with col2:
                y_col = st.selectbox("Y Axis", [c for c in numeric_cols if c != x_col], key="scatter_y")
            with col3:
                color_col = st.selectbox("Color By", [None] + categorical_cols, key="scatter_color")
            
            fig = px.scatter(
                df,
                x=x_col,
                y=y_col,
                color=color_col,
                title=f"{y_col.replace('_', ' ').title()} vs {x_col.replace('_', ' ').title()}",
                color_discrete_sequence=px.colors.qualitative.Vivid,
                template="plotly_dark"
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            
        elif chart_type == "Heatmap" and len(numeric_cols) >= 2:
            st.info("Showing correlation heatmap of numeric columns")
            
            corr_matrix = df[numeric_cols].corr()
            
            fig = px.imshow(
                corr_matrix,
                text_auto=True,
                aspect="auto",
                title="Correlation Heatmap",
                color_continuous_scale="RdBu_r",
                template="plotly_dark"
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        # Data Table with Filters
        st.divider()
        st.subheader("🔍 Filtered Data View")
        
        if categorical_cols:
            filter_col = st.selectbox("Filter by Column", categorical_cols)
            unique_values = df[filter_col].unique()
            selected_values = st.multiselect(
                f"Select {filter_col} values",
                unique_values,
                default=list(unique_values[:5]) if len(unique_values) > 5 else list(unique_values)
            )
            
            if selected_values:
                filtered_df = df[df[filter_col].isin(selected_values)]
                st.dataframe(filtered_df, use_container_width=True, height=400)
                
                # Download button
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Filtered Data as CSV",
                    data=csv,
                    file_name=f"filtered_data_{filter_col}.csv",
                    mime="text/csv"
                )
        else:
            st.dataframe(df, use_container_width=True, height=400)
            
    else:
        st.info("👈 Please load a dataset in the 'Data Preview' tab first to see visualizations.")
        st.markdown("""
        ### Available Summary Tables:
        1. **Overall KPIs**: Daily revenue, units, transactions
        2. **Festival Category**: Performance by festival and product category
        3. **Geo Summary**: City-level sales with coordinates
        4. **Weather Sales**: Sales correlation with weather conditions
        5. **Payment Vendor**: Revenue by payment method and vendor type
        """)

