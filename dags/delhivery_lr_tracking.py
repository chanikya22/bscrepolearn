from datetime import datetime, timedelta
import sys
from airflow import DAG
import pandas as pd
from utils.postgresconnector_v3 import PostgresConnector
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State
from airflow.models import DagRun
import pytz
from utils.pipeline_tracker import PipelineTracker
sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator
from utils.postgresconnector_v3 import PostgresConnector
from connections.bsc.delhivery.lr_tracking.extract import extract_data
from connections.bsc.delhivery.lr_tracking.transform import transform_data
from connections.bsc.delhivery.lr_tracking.load import load_data


ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'ayushgoyal@bombayshavingcompany.com',
    'lakshay@bombayshavingcompany.com',
    'swapnil@bombayshavingcompany.com'
    'shishank@bombayshavingcompany.com',
]
# Default args

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def refresh_b2b_lr_tracking():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """SELECT bsc.refresh_lr_tracking_b2b();"""
    postgres.execute_query(query)

dag = DAG(
    dag_id='delivery_lr_tracking',
    description='delhivery lr tracking ETL',
    schedule_interval='30 3 * * *',  # Daily at 9 AM IST
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'delhivery', 'lr_tracking'],
)


# --- DAG Structure ---
start = EmptyOperator(task_id='start')


# Define the extract task
extract= TrackedPythonOperator(
    task_id='extract_from_source',
    python_callable = extract_data,
    pipeline_name='bsc-delhivery',
    client_id='bsc',
    data_type='bsc-delhivery-tracking',
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag)

transform= TrackedPythonOperator(
    task_id='transform_from_raw',
    python_callable = transform_data,
    pipeline_name='bsc-delhivery',
    client_id='bsc',
    data_type='bsc-delhivery-tracking',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag)

load = TrackedPythonOperator(
    task_id='load_to_iceberg',
    python_callable=load_data,
    pipeline_name='bsc-delhivery',
    client_id='bsc',
    data_type='bsc-delhivery-tracking',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

refresh_b2b_lr_tracking = TrackedPythonOperator(
    task_id='refresh_b2b_lr_tracking',
    python_callable=refresh_b2b_lr_tracking,
    pipeline_name='bsc-delhivery',
    client_id='bsc',
    data_type='bsc-delhivery-tracking',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

end_task = EmptyOperator(task_id='pipeline_complete',dag=dag)

start >> extract >>transform >>load >> refresh_b2b_lr_tracking >> end_task