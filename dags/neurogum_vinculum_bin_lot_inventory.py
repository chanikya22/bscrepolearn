from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
# Import task functions
from connections._100days.neurogum.vinculum.lotbininventory.extract_load import load
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

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
    dag_id='neurogum_vinculum_bin_lot_inventory_connector',
    description='EL pipeline vinculumn bin lot inventory Connector for neurogum',
    schedule_interval="0 * * * *", #every hour
    default_args=default_args,
    start_date=datetime(2026, 2, 27),
    catchup=False,
    tags=['el','vinculumn','bin_lot_inventory', 'connector','neurogum'],
)

# Define the extract task - this might need start_date and end_date params
extraxt_load= TrackedPythonOperator(
    task_id='extract_load_data',
    python_callable=load,
    op_kwargs={
        'brand': 'neurogum'
    },
    pipeline_name='vinculum_bin_lot_inventory',
    client_id='neurogum',
    data_type='bin_lot_inventory_report',
    is_first_task=True,
    dag=dag
)
