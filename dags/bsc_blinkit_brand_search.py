from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

# Import task functions
from connections.insytscraping.blinkit.brandsearch.transform import transform_data
from connections.insytscraping.blinkit.brandsearch.load import load_data
from connections.insytscraping.blinkit.brandsearch.extract import extract_data

# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

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
    dag_id='bsc_blinkit_brand_search',
    description='ETL pipeline for Blinkit Brand Search for BSC',
    schedule_interval='30 3 * * *',  # Daily at 9:00 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=['etl', 'bsc','blinkit', 'brandsearch', 'scraping'],
)
# Define the extract task - this might need start_date and end_date params
extract_task = TrackedPythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={
        'brand_name': 'Bombay Shaving Company',
        'brand_id': 11340,
        'client': 'bsc',
        'locations_csv_s3_path': 'locations.csv',
         's3_bucket': 'bsc-file-automation',
        's3_output_path': 'insytscraping/blinkit/brand_search/',
        'rate_limit': 500,
        'max_workers': None  # Will automatically set based on rate_limit
    },
    pipeline_name='bsc_blinkit_brand_search',
    client_id='bsc',
    data_type='brand-search-scrape',
    is_first_task=True,
    dag=dag
)

# Transform task - NO start_date and end_date params needed
transform_task = TrackedPythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={
        'brand':'BSC',
        'brand_id': 11340,
    },
    pipeline_name='blinkit_brandsearch',
    client_id='bsc',
    data_type='brandsearch',
    is_last_task=False,
    dag=dag
)
load_task = TrackedPythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'brand': 'bsc'},
    pipeline_name='blinkit_brandsearch',
    client_id='bsc',
    data_type='brandsearch',
    is_last_task=True,
    dag=dag
)




# Set dependencies
extract_task >> transform_task >> load_task