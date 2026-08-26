from datetime import datetime, timedelta
import sys
import pandas as pd

sys.path.append('/opt/airflow')

# Import task functions
from connections.insytscraping.blinkit.categorysearch.transform import transform_data
from connections.insytscraping.blinkit.categorysearch.load import load_data
from connections.insytscraping.blinkit.categorysearch.extract import extract_data

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
    dag_id='bsc_blinkit_category_search',
    description='ETL pipeline for Blinkit category Search for BSC',
    schedule_interval='30 0,8,10,14,16 * * *',  # 6AM, 2PM, 4PM, 8PM, 10PM IST
    default_args=default_args,
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=['etl','bsc', 'blinkit', 'categorysearch', 'scraping'],
)

# Define the extract task - this might need start_date and end_date params
extract_task = TrackedPythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={
         'l0_cat_id' : 163,
         'l1_cat_id' : 692,
         'locations_csv_s3_path' : 'locations.csv',
         's3_output_path' : f'insytscraping/blinkit/category_search/year={year}/month={month}/day={day}/l0_cat_163/l1_cat_692/',
         'rate_limit' : 400,
         'max_workers' : None,
        'date': date
    },
    pipeline_name='bsc_blinkit_category_search',
    client_id='bsc',
    data_type='category-search-scrape',
    is_first_task=True,
    dag=dag
)

# Transform task - NO start_date and end_date params needed
transform_task = TrackedPythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    # No op_kwargs needed as transform_data only uses run_id and tracker
    pipeline_name='blinkit_categorysearch',
    client_id='bsc',
    data_type='categorysearch',
    is_last_task=False,
    dag=dag
)
load_task = TrackedPythonOperator(
    task_id='load_data',
    python_callable=load_data,
    #op_kwargs={'config_path': config_path},
    pipeline_name='blinkit_categorysearch',
    client_id='bsc',
    data_type='categorysearch',
    is_last_task=True,
    dag=dag
)




# Set dependencies
extract_task >> transform_task >> load_task