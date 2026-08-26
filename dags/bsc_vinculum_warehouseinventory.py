from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
# Import task functions
from connections.bsc.vinculum.warehouseinventory.extract_load import load
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

from connections.bsc.vinculum.soda_health_check.run_soda import run_scan as run_soda_scan


ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'ayushgoyal@bombayshavingcompany.com',
    'lakshay@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',
    'akshat@bombayshavingcompany.com',
]



# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 6,
    'retry_delay': timedelta(minutes=1),
}

def data_freshness_checks_audit():
    run_soda_scan(
        "freshness_audit",
        "audit_db",
        ["checks/audit_db/data_freshness_checks.yml"]
    )

def data_freshness_checks_warehouse():
    run_soda_scan(
        "freshness_vinculum_inventory",
        "warehouse_db",
        ["checks/warehouse_db/vinculum_inventory_v2.yml"]
    )

# Create DAG directly
dag = DAG(
    dag_id='vinculum_warehouseinventory',
    description='ETL pipeline vinculumn warehouseinventory',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    default_args=default_args,
    start_date=datetime(2025, 11, 20),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['etl','vinculumn','warehouseinventory'],
)

# Define the extract task - this might need start_date and end_date params
extraxt_load= TrackedPythonOperator(
    task_id='extract_load_data',
    python_callable=load,
    op_kwargs={
        'brand': 'bsc'
    },
    pipeline_name='bsc-vinculum-bin-lot-inventory',
    client_id='bsc',
    data_type='warehouse_inventory_report',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    dag=dag
)

data_freshness_checks_audit_task = TrackedPythonOperator(
    task_id='data_freshness_checks_audit',
    python_callable=data_freshness_checks_audit,
    pipeline_name='bsc-vinculum-bin-lot-inventory',
    client_id='bsc',
    data_type='warehouse_inventory_report',
    is_first_task=False,    
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

data_freshness_checks_warehouse_task = TrackedPythonOperator(
    task_id='data_freshness_checks_warehouse',
    python_callable=data_freshness_checks_warehouse,
    pipeline_name='bsc-vinculum-bin-lot-inventory',
    client_id='bsc',
    data_type='warehouse_inventory_report',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

# Set task dependencies
extraxt_load >> data_freshness_checks_audit_task >> data_freshness_checks_warehouse_task


