
from datetime import datetime, timedelta
from tracemalloc import start
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from plugins.operators.tracked_python_operator import TrackedPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from connections._100days.dfm.blinkit.sales.extract_load import load
from connections._100days.dfm.soda_health_check.run_soda import run_scan as run_soda_scan
ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'ayushgoyal@bombayshavingcompany.com',
    'lakshay@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',
]

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 15,
    'retry_delay': timedelta(minutes=1),
}

# Create DAG directly
dag = DAG(
    dag_id='dfm_blinkit_sales',
    description='Pipeline for dfm sales of blinkit',
    schedule_interval='0 0 * * *', # Daily at 5 30  AM IST
    default_args=default_args,
    start_date=datetime(2025, 11, 20),
    catchup=False,
    tags=['etl','100days', 'dfm'],
)

def data_freshness_checks_audit():
    run_soda_scan(
        "freshness_audit",
        "audit_db",
        ["checks/audit_db/data_freshness_checks.yaml"]
    )

start = EmptyOperator(task_id='start')


# Define the extract task - this might need start_date and end_date params
extract_load = TrackedPythonOperator(
    task_id='sales_report',
    python_callable = load,
    op_kwargs={
        'brand': 'dfm',},
    pipeline_name='dfm-blinkit-sales',
    client_id='dfm',
    data_type='dfm-blinkit-sales',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag)


data_freshness_checks_audit_task = TrackedPythonOperator(
    task_id='data_freshness_checks_audit',
    python_callable=data_freshness_checks_audit,
    pipeline_name='dfm-blinkit-sales',
    client_id='dfm',
    data_type='dfm-blinkit-sales',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

end = EmptyOperator(task_id='end')


# Set task dependencies
start >> extract_load >> data_freshness_checks_audit_task >> end


