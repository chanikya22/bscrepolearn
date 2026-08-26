from datetime import datetime, timedelta
from tracemalloc import start
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from plugins.operators.tracked_python_operator import TrackedPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from connections.bsc.shopify.shopify_health_check import validate_google_spent, validate_facebook_spent, validate_data_freshness, validate_GA4_sessions

ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'ayushgoyal@bombayshavingcompany.com',
    'lakshay@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',
    
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'catchup': False  # This prevents backfilling
}

dag = DAG(
    dag_id='shopify_health_check',
    description='Shopify pipeline health check BSC',
    schedule_interval='45 1 * * *',  # Daily at 9 AM IST
    default_args=default_args,
    start_date=datetime(2025, 7, 9),
    catchup=False,
    tags=['etl', 'shopify', 'pnl', 'health-check'],
)

start = EmptyOperator(task_id='start')

validate_data_freshness_task = TrackedPythonOperator(
    task_id='validate_data_freshness',
    python_callable=validate_data_freshness,
    pipeline_name='shopify-health-check',
    client_id='bsc',
    data_type='health-check',
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

validate_google_spent_task = TrackedPythonOperator(
    task_id='validate_google_spent',
    python_callable=validate_google_spent,
    pipeline_name='shopify-health-check',
    client_id='bsc',
    data_type='health-check',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)
validate_facebook_spent_task = TrackedPythonOperator(
    task_id='validate_facebook_spent',
    python_callable=validate_facebook_spent,
    pipeline_name='shopify-health-check',
    client_id='bsc',
    data_type='health-check',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

validate_GA4_sessions_task = TrackedPythonOperator(
    task_id='validate_GA4_sessions',
    python_callable=validate_GA4_sessions,
    pipeline_name='shopify-health-check',
    client_id='bsc',
    data_type='health-check',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,  
    success_email_to=ALERT_EMAILS,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

end = EmptyOperator(task_id='end')

start >> validate_data_freshness_task >> validate_google_spent_task >> validate_facebook_spent_task >> validate_GA4_sessions_task >> end

