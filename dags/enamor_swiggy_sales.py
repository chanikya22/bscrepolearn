import sys

sys.path.append('/opt/airflow')

# Import task functions
from connections.enamor.swiggy.sales.load import load_data
from connections.enamor.swiggy.sales.extract import extract_data

# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator
from datetime import datetime, timedelta

current_date = datetime.now()

# Get yesterday
yesterday = current_date - timedelta(days=1)

# Start of the month of yesterday (as datetime at midnight)
start_date = datetime(yesterday.year, yesterday.month, 1)

# End date is yesterday (as datetime at midnight)
end_date = datetime(yesterday.year, yesterday.month, yesterday.day)

# Print date range
print(f"Date range: {start_date.date()} to {end_date.date()}")

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
    dag_id='enamor_swiggy_sales_connector',
    description='ETL pipeline for  Swiggy Sales Connector for enamor',
    schedule_interval='30 9 * * *',  # Daily at 8:00 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 9, 11),
    catchup=False,
    tags=['el', 'enamor','swiggy', 'Sales', 'connector'],
)

# Define the extract task - this might need start_date and end_date params
extract_task = TrackedPythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={
        'brand': 'enamor',
        'start_date': start_date,
        'end_date':end_date,
        'file_upload_path': 'swiggy/enamor/sales-report/',
    },
    pipeline_name='enamor_swiggy_sales',
    client_id='enamor',
    data_type='swiggy_sales',
    is_first_task=True,
    dag=dag
)

load_task = TrackedPythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'brand': 'enamor'},
    pipeline_name='swiggy_sales',
    client_id='enamor',
    data_type='swiggy_sales',
    is_last_task=True,
    dag=dag
)




# Set dependencies
extract_task >>load_task