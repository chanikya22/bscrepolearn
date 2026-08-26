from datetime import datetime, timedelta
import sys
import pandas as pd

sys.path.append('/opt/airflow')

# Import task functions
from connections._100days.modenik.soda_health_check.run_soda import run_scan as run_soda_scan

# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'ayushgoyal@bombayshavingcompany.com',
    'swapnil@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',
]


# #========================================================================
# Checks definations
# #========================================================================

def data_freshness_checks():
    run_soda_scan(
        "freshness_audit",
        "audit_db",
        ["checks/audit_db/data_freshness_checks.yaml"]
    )

def blinkit_checks():
    run_soda_scan(
        "blinkit_checks",
        "warehouse_db",
        ["checks/warehouse_db/blinkit_checks.yaml"]
    )

def swiggy_checks():
    run_soda_scan(
        "swiggy_checks",
        "warehouse_db",
        ["checks/warehouse_db/swiggy_checks.yaml"]
    )

def zepto_checks():
    run_soda_scan(
        "zepto_checks",
        "warehouse_db",
         ["checks/warehouse_db/zepto_checks.yaml"]
    )



# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='modenik_soda_checks',
    description='modenik pipeline health check ',
    schedule_interval='0 3 * * *',  # Daily at 8:30 AM IST (3:00 AM UTC)
    default_args=default_args,
    start_date=datetime(2025, 7, 9),
    catchup=False,
    tags=['sales', 'modenik','health-check'],
)

data_freshness_task = TrackedPythonOperator(
    task_id='data_freshness_checks',
    python_callable=data_freshness_checks,
    pipeline_name='modenik-soda-checks',
    client_id='modenik',
    data_type='health-check',
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)


zepto_checks_task = TrackedPythonOperator(
    task_id='zepto_sales_checks',
    python_callable=zepto_checks,
    pipeline_name='modenik-soda-checks',
    client_id='modenik',
    data_type='health-check',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)


blinkit_checks_task = TrackedPythonOperator(
    task_id='blinkit_sales_checks',
    python_callable=blinkit_checks,
    pipeline_name='modenik-soda-checks',
    client_id='modenik',
    data_type='health-check',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)


swiggy_checks_task = TrackedPythonOperator(
    task_id='swiggy_sales_checks',
    python_callable=swiggy_checks,
    pipeline_name='modenik-soda-checks',
    client_id='modenik',
    data_type='health-check',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

data_freshness_task >>zepto_checks_task >> blinkit_checks_task >> swiggy_checks_task