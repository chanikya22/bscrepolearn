from datetime import datetime, timedelta
import sys
from airflow.operators.python import PythonOperator
sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG
from connections.bsc.nykaa.ads.extract_load import load

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
    dag_id='nykaa_sales_report',
    description='pipeline to extract and load nykaa sales report',
    schedule_interval='45 4 * * *',  # Daily at 8:15 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 12, 12),
    catchup=False,
    tags=['nykaa','sales'],
)

# Define the extract task - this might need start_date and end_date params
login_task = PythonOperator(
    task_id='ads_report',
    python_callable = load,
    op_kwargs={
        'brand': 'bsc',},
    dag=dag)
