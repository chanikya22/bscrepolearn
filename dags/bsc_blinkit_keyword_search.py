from datetime import datetime, timedelta
import sys
import pandas as pd
import gspread
from utils.google_connector import  connect_to_google_sheet
sys.path.append('/opt/airflow')

# Import task functions
from connections.insytscraping.blinkit.keywordsearch.transform import transform_data
from connections.insytscraping.blinkit.keywordsearch.load import load_data
from connections.insytscraping.blinkit.keywordsearch.extract import extract_data

# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

date = pd.Timestamp(datetime.now())
# Extract individual components
year = date.year
month = date.month
day = date.day
time_str = date.strftime("%H:%M:%S")

def get_keywords_data():
    client = connect_to_google_sheet()
    spreadsheet = client.open_by_key('1bBKM2bYXnqzqx_9iWVcvHOphhMk3QLuwQ0X9IuUdn0M')
    sheet = spreadsheet.worksheet('bsc_blinkit_keywords')
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    keyword_list = df['keyword'].dropna().tolist()  # remove NaNs if any
    processed_keywords = [k.strip().replace(' ', '+') for k in keyword_list]
    return processed_keywords
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
    dag_id='bsc_blinkit_keyword_search',
    description='ETL pipeline for Blinkit keyword Search for BSC',
    schedule_interval='30 4 * * *',  # Daily at 10:00 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=['etl','bsc' ,'blinkit', 'keywordsearch', 'scraping'],
)

# Define the extract task - this might need start_date and end_date params


extract_task = TrackedPythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={
          'search_keywords': get_keywords_data(),
         'locations_csv_s3_path' : 'keyword_search_locations.csv',
         's3_output_path' : f'insytscraping/blinkit/keyword_search/year={year}/month={month}/day={day}/',
         'rate_limit' : 400,
         'max_workers' : None,
        'date': date
    },
    pipeline_name='bsc_blinkit_keyword_search',
    client_id='bsc',
    data_type='keyword-search-scrape',
    is_first_task=True,
    dag=dag
)

# Transform task - NO start_date and end_date params needed
transform_task = TrackedPythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'run_type': "{{ 'automatic' if dag_run.run_type == 'scheduled' else 'manual' }}"},
    pipeline_name='blinkit_keywordsearch',
    client_id='bsc',
    data_type='keywordsearch',
    is_last_task=False,
    dag=dag
)
load_task = TrackedPythonOperator(
    task_id='load_data',
    python_callable=load_data,
    #op_kwargs={'config_path': config_path},
    pipeline_name='blinkit_keywordsearch',
    client_id='bsc',
    data_type='keywordsearch',
    is_last_task=True,
    dag=dag
)




# Set dependencies
extract_task >> transform_task >> load_task