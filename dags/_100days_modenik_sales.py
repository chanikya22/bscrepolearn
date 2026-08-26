from datetime import datetime, timedelta
import sys
from plugins.operators.tracked_python_operator import TrackedPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.postgresconnector_v3 import PostgresConnector
from connections._100days.modenik.blinkit.sales.extract_load import load as blinkit_load
from connections._100days.modenik.swiggy.sales.extract_load import load as swiggy_load
from connections._100days.modenik.zepto.sales.extract_load import load as zepto_load

def upsert_sales_summary():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = f"""CALL modenik.upsert_qcom_sales_summary(); """
    postgres.execute_query(query)

ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'ayushgoyal@bombayshavingcompany.com',
    'swapnil@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',
]

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
}

# Create DAG directly
dag = DAG(
    dag_id='_100days_modenik_Sales',
    description='pipeline to send sales data of modenik',
    schedule_interval='30 2 * * *', # Daily at 7:00 AM IST
    default_args=default_args,
    start_date=datetime(2025, 12, 17),
    catchup=False,
    tags=['Blinkit','Zepto','Swiggy','modenik','100days'],
)


start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Define the extract task
zepto_sales_load_task = TrackedPythonOperator(
    task_id='zepto_sales_load',
    python_callable = zepto_load,
    op_kwargs={
        'brand': 'modenik',},
    pipeline_name='modenik-zepto-sales',
    client_id='modenik',
    data_type='modenik_zepto_sales',
    is_first_task=True,
    is_last_task =True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag)

swiggy_sales_load_task = TrackedPythonOperator(
    task_id='swiggy_sales_load',
    python_callable = swiggy_load,
    op_kwargs={
        'brand': 'modenik',},
    pipeline_name='modenik-swiggy-sales',
    client_id='modenik',
    data_type='modenik_swiggy_sales',
    is_first_task=True,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag)

blinkit_sales_load_task =TrackedPythonOperator(
    task_id='blinkit_sales_load',
   python_callable = blinkit_load,
    op_kwargs={
        'brand': 'modenik',},
    pipeline_name='modenik-blinkit-sales',
    client_id='modenik',
    data_type='modenik_blinkit_sales',
    is_first_task=True,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag)

upsert_sales_summary_task = PythonOperator(
    task_id="upsert_sales_summary_task",
    python_callable=upsert_sales_summary,
    dag=dag)

# End marker
end_task = DummyOperator(
    task_id='end_pipeline',
    trigger_rule='none_failed_min_one_success',  # Continue even if some tasks fails
    dag=dag
)


start >> zepto_sales_load_task >> upsert_sales_summary_task >> end_task

start >> swiggy_sales_load_task >>upsert_sales_summary_task >> end_task

start >> blinkit_sales_load_task >> upsert_sales_summary_task >> end_task