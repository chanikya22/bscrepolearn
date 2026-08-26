from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

# Import task functions
from connections.ryze.blinkit.sales.load import load_data
from connections.ryze.blinkit.sales.extract import extract_data

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
    dag_id='ryze_blinkit_sales_connector',
    description='ETL pipeline for  Blinkit Sales Connector for Ryze',
    schedule_interval='30 9 * * *',  # Daily at 8:00 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 8, 13),
    catchup=False,
    tags=['el', 'ryze','blinkit', 'Sales', 'connector'],
)

# Define the extract task - this might need start_date and end_date params
extract_task = TrackedPythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={
        'brand': 'ryze',
        'file_upload_path': 'blinkit/ryze/sales-report/',
    },
    pipeline_name='ryze_blinkit_sales',
    client_id='ryze',
    data_type='blinkit_sales',
    is_first_task=True,
    dag=dag
)

load_task = TrackedPythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'brand': 'ryze'},
    pipeline_name='blinkit_sales',
    client_id='ryze',
    data_type='blinkit_sales',
    is_last_task=True,
    dag=dag
)




# Set dependencies
extract_task >>load_task