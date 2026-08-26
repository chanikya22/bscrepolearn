from datetime import datetime, timedelta
import sys
from airflow.operators.python import PythonOperator
sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG
from connections.bsc.amazon.sellercentral.sqp.extract_load import load

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
    dag_id='Amazon_sellercentral_sqp_report',
    description='pipeline to extract and load amazon sellercentral sqp report',
    schedule_interval='45 1 * * *',  # Daily at 6:15 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 12, 4),
    catchup=False,
    tags=['amazon','sellercentral','sqp'],
)

# Define the extract task - this might need start_date and end_date params
login_task = PythonOperator(
    task_id='sqpreport',
    python_callable = load,
    op_kwargs={
        'brand': 'bsc',},
    dag=dag)
