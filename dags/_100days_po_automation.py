from datetime import datetime, timedelta
import sys
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG

from connections._100days.modenik.blinkit.po.extract_load import load as blinkit_po_load
from connections._100days.modenik.swiggy.po.extract_load import load as swiggy_po_load
from connections._100days.modenik.zepto.po.extract_load import load as zepto_po_load


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
    dag_id='_100days_po_automation',
    description='pipeline to send dsr',
    schedule_interval='0 0 * * *',  # Daily at 5 30  AM IST
    default_args=default_args,
    start_date=datetime(2025, 12, 17),
    catchup=False,
    tags=['Blinkit','Zepto','Swiggy','po','100days'],
)


start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Define the extract task - this might need start_date and end_date params
zepto_po_load_task = PythonOperator(
    task_id='zepto_po_load',
   python_callable = zepto_po_load,
    op_kwargs={
        'brand': 'modenik',},
    dag=dag)

swiggy_po_load_task = PythonOperator(
    task_id='swiggy_po_load',
   python_callable = swiggy_po_load,
    op_kwargs={
        'brand': 'modenik',},
    dag=dag)

blinkit_po_load_task = PythonOperator(
    task_id='blinkit_po_load',
   python_callable = blinkit_po_load,
    op_kwargs={
        'brand': 'modenik',},
    dag=dag)

# End marker
end_task = DummyOperator(
    task_id='end_pipeline',
    trigger_rule='none_failed_min_one_success',  # Continue even if some tasks fails
    dag=dag
)


start >> zepto_po_load_task >> end_task

start >> swiggy_po_load_task >> end_task

start >> blinkit_po_load_task >> end_task