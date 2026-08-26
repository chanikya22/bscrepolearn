from datetime import datetime, timedelta
import sys
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from plugins.operators.tracked_python_operator import TrackedPythonOperator
sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG
from utils.postgresconnector_v3 import PostgresConnector

from connections.bsc.blinkit.po.extract_load import load as blinkit_po_load
from connections.bsc.swiggy.po.extract_load import load as swiggy_po_load
from connections.bsc.zepto.po.extract_load import load as zepto_po_load

ALERT_EMAILS = [
    
    'ayushgoyal@bombayshavingcompany.com',
    'lakshay@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',
    'sujal@bombayshavingcompany.com'
    
]

def refresh_qcom_fillrate():
    postgres = PostgresConnector(db_prefix="warehouse_")

    query = """
                select * from bsc.refresh_qcom_fill_rate()
            """

    postgres.execute_query(query) 

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
    dag_id='bsc_po_automation',
    description='pipeline to send dsr',
    schedule_interval='55 9 * * *',  # Daily at 8:25 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 12, 17),
    catchup=False,
    tags=['Blinkit','Zepto','Swiggy','po','bsc'],
)

# Define the extract task - this might need start_date and end_date params
zepto_po_load_task = TrackedPythonOperator(
    task_id='zepto_po_load',
    python_callable = zepto_po_load,
    op_kwargs={
        'brand': 'bsc',},
    pipeline_name="po_automation_pipeline",
    client_id="bsc",
    data_type="po_automation_pipeline",
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    dag=dag)

swiggy_po_load_task = TrackedPythonOperator(
    task_id='swiggy_po_load',
    python_callable = swiggy_po_load,
    op_kwargs={
        'brand': 'bsc',},
    pipeline_name="po_automation_pipeline",
    client_id="bsc",
    data_type="po_automation_pipeline",
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    dag=dag)

blinkit_po_load_task = TrackedPythonOperator(
    task_id='blinkit_po_load',
    python_callable = blinkit_po_load,
    op_kwargs={
        'brand': 'bsc',},
    pipeline_name="po_automation_pipeline",
    client_id="bsc",
    data_type="po_automation_pipeline",
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    dag=dag)


# Define the refresh task
refresh_qcom_fillrate_task = TrackedPythonOperator(
    task_id='refresh_qcom_fillrate',
    python_callable=refresh_qcom_fillrate,
    pipeline_name="po_automation_pipeline",
    client_id="bsc",
    data_type="po_automation_pipeline",
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',  # Run even if blinkit/zepto/swiggy tasks fail
    dag=dag)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# End marker  
end_task = EmptyOperator(
    task_id='end_pipeline',
    trigger_rule='none_failed_min_one_success',  # Continue even if some tasks fails
    dag=dag
)


start >> zepto_po_load_task >> refresh_qcom_fillrate_task >> end_task

start >> swiggy_po_load_task >> refresh_qcom_fillrate_task >> end_task

start >> blinkit_po_load_task >> refresh_qcom_fillrate_task >> end_task