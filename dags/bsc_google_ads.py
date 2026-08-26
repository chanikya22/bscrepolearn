from datetime import datetime, timedelta
import sys
import pandas as pd

sys.path.append('/opt/airflow')

# Import task functions
from connections.bsc.shopify.google_ads.google_bsc.extract import extract_data
from connections.bsc.shopify.google_ads.google_bsc.load import load_data

# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

date = pd.Timestamp(datetime.now())
# Extract individual components
year = date.year
month = date.month
day = date.day
time_str = date.strftime("%H:%M:%S")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG directly
dag = DAG(
    dag_id='bsc_google_ads_pipeline',
    description='ETL pipeline for Google Ads data extraction and loading for BSC',
    schedule_interval='0 0 * * *',  # Daily at 5 30  AM IST
    default_args=default_args,
    start_date=datetime(2025, 7, 9),
    catchup=False,
    tags=['etl', 'bsc', 'google-ads', 'marketing', 'api'],
)

# Define the extract task
extract_task = TrackedPythonOperator(
    task_id='extract_google_ads_data',
    python_callable=extract_data,
    op_kwargs={
        'report_types': ['campaigns', 'keywords', 'ads']  # Extract all report types else mention specific ones
    },
    pipeline_name='google-ads',
    client_id='bsc',
    data_type='marketing-data',
    is_first_task=True,
    dag=dag
)

# Load task - loads the extracted data to database
load_task = TrackedPythonOperator(
    task_id='load_google_ads_data',
    python_callable=load_data,
    # No op_kwargs needed as load_data only uses run_id and tracker
    pipeline_name='google-ads',
    client_id='bsc',
    data_type='marketing-data',
    is_last_task=True,
    dag=dag
)

# Set dependencies - Extract then Load (no transform step)
extract_task >> load_task