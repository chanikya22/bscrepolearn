from datetime import datetime, timedelta
import sys
import pandas as pd
from utils.postgresconnector_v3 import PostgresConnector

sys.path.append('/opt/airflow')

# Import task functions
from connections.bsc.vinculum.soda_health_check.run_soda import run_scan as run_soda_scan


# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'ayushgoyal@bombayshavingcompany.com',
    'lakshay@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',
    
]
# #========================================================================
# Checks definations
# #========================================================================

def data_freshness_checks():
    run_soda_scan(
        "freshness_audit",
        "audit_db",
        ["checks/audit_db/data_freshness_checks.yml"]
    )


def run_vinculum_casefill():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """
        SELECT * FROM bsc.refresh_vinculum_casefill();
    """
    postgres.execute_query(query)



# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='vinculum_soda_checks',
    description='Vinculum pipeline health check BSC',
    schedule_interval='0 */2 * * *',  # EVERY 2 HOURS
    default_args=default_args,
    start_date=datetime(2025, 7, 9),
    catchup=False,
    tags=['etl', 'shopify', 'pnl', 'health-check'],
)

data_freshness_task = TrackedPythonOperator(
    task_id='data_freshness_checks',
    python_callable=data_freshness_checks,
    pipeline_name='vinculum-soda-checks',
    client_id='bsc',
    data_type='health-check',
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

vinculum_casefill_task = TrackedPythonOperator(
    task_id='run_vinculum_casefill_refresh',
    python_callable=run_vinculum_casefill,
    pipeline_name='vinculum-soda-checks',
    client_id='bsc',
    data_type='health-check',
    is_first_task=False,
    is_last_task=True,
    dag=dag
)

data_freshness_task >> vinculum_casefill_task
