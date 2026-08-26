from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

# Import task functions
from connections.bsc.gokwik.settlement.load import load_data
from connections.bsc.gokwik.settlement.extract import generate_gokwik_report

from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator
account_bombae = "3mt5u7t4ejl2a88iwu"
account_vlpcpl ="3mt5u7n3w2kzqua196"
merchant_id_bombae= 347 # Bombae
merchant_id_vlpcpl= 290  #BSC
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
    dag_id='gokwik_settelment_connector',
    description='EL pipeline for GoKwik Settelment Connector for bsc',
    schedule_interval='30 1 * * *',  # Daily at 7:00 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 11, 19),
    catchup=False,
    tags=['el','Gokwik','settlement', 'connector'],
)

# Define the extract task - this might need start_date and end_date params
extract_task_bsc = TrackedPythonOperator(
    task_id='extract_data_bsc',
    python_callable=generate_gokwik_report,
    op_kwargs={
        'merchant_id': merchant_id_vlpcpl,
        'account_id':account_vlpcpl,
        'brand': 'bsc',
        's3_bucket': 'bsc-file-automation',
    },
    pipeline_name='gokwik_settelment',
    client_id='bsc',
    data_type='settelment_report',
    is_first_task=True,
    dag=dag
)

load_task_bsc = TrackedPythonOperator(
    task_id='load_data_bsc',
    python_callable=load_data,
    pipeline_name='gokwik_settelment',
    client_id='bsc',
    data_type='settelment_report',
    is_last_task=True,
    dag=dag
)

extract_task_bombae = TrackedPythonOperator(
    task_id='extract_data_bombae',
    python_callable=generate_gokwik_report,
    op_kwargs={
        'merchant_id' : merchant_id_bombae,
        'account_id':account_bombae,
        'brand': 'bombae',
        's3_bucket': 'bsc-file-automation',
    },
    pipeline_name='gokwik_settelment',
    client_id='bsc',
    data_type='settelment_report',
    is_first_task=True,
    dag=dag
)

load_task_bombae = TrackedPythonOperator(
    task_id='load_data_bombae',
    python_callable=load_data,
    pipeline_name='gokwik_settelment',
    client_id='bsc',
    data_type='settelment_report',
    is_last_task=True,
    dag=dag
)




# Set dependencies
extract_task_bsc >>load_task_bsc >>extract_task_bombae >>load_task_bombae