from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
# Import task functions
from connections.bsc.vinculum.lotbininventory.extract_load import load
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
    dag_id='vinculum_bin_lot_inventory_connector',
    description='EL pipeline vinculumn bin lot inventory Connector for bsc',
    schedule_interval="*/10 * * * *", #every 10 minute
    default_args=default_args,
    start_date=datetime(2025, 11, 20),
    catchup=False,
    tags=['el','vinculumn','bin_lot_inventory', 'connector'],
)

# Define the extract task - this might need start_date and end_date params
extraxt_load= TrackedPythonOperator(
    task_id='extract_load_data',
    python_callable=load,
    op_kwargs={
        'brand': 'bsc'
    },
    pipeline_name='vinculum_bin_lot_inventory',
    client_id='bsc',
    data_type='bin_lot_inventory_report',
    is_first_task=True,
    dag=dag
)
