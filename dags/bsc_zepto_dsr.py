from datetime import datetime, timedelta
import sys
from airflow.operators.python import PythonOperator
sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG
from connections.bsc.zepto.sales.extract_load import load as sales_load
from connections.bsc.zepto.ads.extract_load import load as ads_load
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
    dag_id='Zepto_DSR',
    description='pipeline to send dsr',
    schedule_interval='55 1 * * *',  # Daily at 6:25 AM (IST)
    default_args=default_args,
    start_date=datetime(2025, 12, 17),
    catchup=False,
    tags=['Zepto','dsr','bsc'],
)

# Define the extract task - this might need start_date and end_date params
load_sales_task = PythonOperator(
    task_id='load_sales',
   python_callable = sales_load,
    op_kwargs={
        'brand': 'bsc'},
    dag=dag)

load_ads_bombae_task = PythonOperator(
    task_id='load_bombae_ads',
   python_callable = ads_load,
    op_kwargs={
        'brand': 'Bombae',
        'brand_id': '0eca916c-cd17-44b3-acb0-158a70a8bfdb'},
    dag=dag)
load_ads_bsc_task = PythonOperator(
    task_id='load_bsc_ads',
   python_callable = ads_load,
    op_kwargs={
        'brand': 'bsc',
        'brand_id': '8ab08085-714a-4a32-aab6-7a10c61af1fc'
    },
    dag=dag)

process_dsr_task = PythonOperator(
    task_id='process_dsr',
   python_callable = process_dsr,
    op_kwargs={
        'channel': 'Zepto',},
    dag=dag)

generate_dsr_task = PythonOperator(
    task_id='generate_dsr',
   python_callable = generate_dsr,
    op_kwargs={
        'channel': 'Zepto',
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

load_sales_task >> load_ads_bombae_task >> load_ads_bsc_task >> process_dsr_task >> generate_dsr_task >> process_central_dsr_dump_task >> refresh_business_overview_task