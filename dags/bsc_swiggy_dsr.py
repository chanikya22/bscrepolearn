from datetime import datetime, timedelta
import sys
from airflow.operators.python import PythonOperator
sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG
from connections.bsc.swiggy.sales.extract_load import load as sales_load
from connections.bsc.swiggy.ads.extract_load import load as ads_load
from connections.bsc.dsr.process_generate import process_dsr , generate_dsr,refresh_business_overview ,process_central_dsr_dump
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
    dag_id='Swiggy_DSR',
    description='pipeline to send dsr',
    schedule_interval='55 2 * * *',  # Daily at 7:25 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 12, 17),
    catchup=False,
    tags=['Swiggy','dsr','bsc'],
)

# Define the extract task - this might need start_date and end_date params
load_sales_task = PythonOperator(
    task_id='load_sales',
   python_callable = sales_load,
    op_kwargs={
        'brand': 'bsc',},
    dag=dag)

load_ads_task = PythonOperator(
    task_id='load_ads',
   python_callable = ads_load,
    op_kwargs={
        'brand': 'bsc',},
    dag=dag)

process_dsr_task = PythonOperator(
    task_id='process_dsr',
   python_callable = process_dsr,
    op_kwargs={
        'channel': 'Swiggy',},
    dag=dag)

generate_dsr_task = PythonOperator(
    task_id='generate_dsr',
   python_callable = generate_dsr,
    op_kwargs={
        'channel': 'Swiggy',
         'devOrProd': '1'},
    dag=dag)

process_central_dsr_dump_task = PythonOperator(
    task_id='process_central_dsr_dump',
   python_callable = process_central_dsr_dump,
    dag=dag)

refresh_business_overview_task = PythonOperator(
    task_id='refresh_business_overview',
   python_callable = refresh_business_overview,
    dag=dag)

load_sales_task >> load_ads_task >> process_dsr_task >> generate_dsr_task >> process_central_dsr_dump_task >> refresh_business_overview_task