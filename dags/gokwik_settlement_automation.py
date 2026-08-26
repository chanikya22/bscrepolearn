from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')
from airflow.operators.empty import EmptyOperator

from connections.bsc.gokwik.settlement.portal_login import extract, load

from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

ALERT_EMAILS = [
    "manish.p@bombayshavingcompany.com",
    "tech@bombayshavingcompany.com",
]


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='gokwik_settelment_automation',
    description='EL pipeline for GoKwik Settlement (Playwright) for bsc',
    schedule_interval='30 3 * * *',   
    default_args=default_args,
    start_date=datetime(2025, 11, 19),
    catchup=False,
    tags=['Gokwik', 'settlement', 'connector'],
)

start = EmptyOperator(task_id="start")

extract_task = TrackedPythonOperator(
    task_id='extract_data',
    python_callable=extract,
    pipeline_name='gokwik_settelment_automation',
    client_id='bsc',
    data_type='settelment_report',
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    dag=dag,
)

load_task = TrackedPythonOperator(
    task_id='load_data',
    python_callable=load,
    pipeline_name='gokwik_settelment_automation',
    client_id='bsc',
    data_type='settelment_report',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    dag=dag,
)

end = EmptyOperator(task_id="end")

start >> extract_task >> load_task >> end