from datetime import datetime, timedelta
import sys
from airflow.operators.python import PythonOperator

sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG
from connections.bsc.gobblecube.osa.extract_load import load
from plugins.operators.tracked_python_operator import TrackedPythonOperator

ALERT_EMAILS = [
    'manish.p@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',

]

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG directly
dag = DAG(
    dag_id='Gobblecube_osa_report',
    description='pipeline to extract and load gobblecube osa report',
    schedule_interval='1 0 * * *',  # Daily at 5:31 AM (IST)
    default_args=default_args,
    start_date=datetime(2026, 1, 6),
    catchup=False,
    tags=['gobblecube', 'osa', 'bsc'],
)

# Define the extract task - this might need start_date and end_date params
blinkit_osa_bsc_report = TrackedPythonOperator(
    task_id='osa_report_bsc_blinkit',
    python_callable=load,
    op_kwargs={
        'platform': 'Blinkit',
        'brand': 'bsc', },
    failure_email_to=ALERT_EMAILS,
    dag=dag)

instamart_osa_bsc_report = TrackedPythonOperator(
    task_id='osa_report_bsc_instamart',
    python_callable=load,
    op_kwargs={
        'platform': 'Instamart',
        'brand': 'bsc', },
    failure_email_to=ALERT_EMAILS,
    dag=dag)

zepto_osa_bsc_report = TrackedPythonOperator(
    task_id='osa_report_bsc_zepto',
    python_callable=load,
    op_kwargs={
        'platform': 'Zepto',
        'brand': 'bsc', },
    failure_email_to=ALERT_EMAILS,
    dag=dag)

blinkit_osa_bombae_report = TrackedPythonOperator(
    task_id='osa_report_bombae_blinkit',
    python_callable=load,
    op_kwargs={
        'platform': 'Blinkit',
        'brand': 'bombae', },
    failure_email_to=ALERT_EMAILS,
    dag=dag)

instamart_osa_bombae_report = TrackedPythonOperator(
    task_id='osa_report_bombae_instamart',
    python_callable=load,
    op_kwargs={
        'platform': 'Instamart',
        'brand': 'bombae', },
    failure_email_to=ALERT_EMAILS,
    dag=dag)

zepto_osa_bombae_report = TrackedPythonOperator(
    task_id='osa_report_bombae_zepto',
    python_callable=load,
    op_kwargs={
        'platform': 'Zepto',
        'brand': 'bombae', },
    failure_email_to=ALERT_EMAILS,
    dag=dag)

blinkit_osa_bsc_report >> instamart_osa_bsc_report >> zepto_osa_bsc_report >> blinkit_osa_bombae_report >> instamart_osa_bombae_report >> zepto_osa_bombae_report