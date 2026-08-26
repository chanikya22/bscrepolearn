from datetime import datetime, timedelta
import sys
from airflow.operators.python import PythonOperator
sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG
from connections.bsc.amazon.sellercentral.fba_sales_tax.extract_load import load

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
    dag_id='Amazon_sellercentral_fba_sales_taxreport',
    description='pipeline to extract and load amazon sellercentral fba_sales_tax sales tax report',
    schedule_interval='35 2 * * *',  # Daily at 7:05 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 12, 4),
    catchup=False,
    tags=['amazon','sellercentral','fba_sales_tax','sales','taxreport'],
)

# Define the extract task - this might need start_date and end_date params
login_task = PythonOperator(
    task_id='fba_sales_taxreport',
    python_callable = load,
    op_kwargs={
        'brand': 'BSC'
    },
    dag=dag)
