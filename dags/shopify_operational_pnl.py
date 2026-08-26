from datetime import datetime, timedelta
import sys
import pandas as pd
import logging
from airflow.operators.empty import EmptyOperator
sys.path.append('/opt/airflow')
# Import task functions
from connections.bsc.shopify.shopify_pnl.load import load, load_view_pipeline , refresh_operational_pnl_order_details ,refresh_shopify_affiliate_validation,refresh_shopify_discount_performance,refresh_shopify_marketplace_summary,refresh_shopify_product_location,refresh_shopify_pnl_combined,refresh_shopify_sales_analysis_v2
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator
# Get current date components







# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False  # This prevents backfilling
}

# Create DAG directly
dag = DAG(
    dag_id='shopify_operational_pnlv2',
    description='ETL pipeline for Shopify PnL data for BSC',
    schedule_interval='30 0 * * *',  # Daily at 6 AM IST
    default_args=default_args,
    start_date=datetime(2025, 7, 9),
    catchup=False,
    tags=['etl', 'shopify', 'pnl'],
)


start = EmptyOperator(task_id='start')
# Define the load task

load_task = TrackedPythonOperator(
    task_id='load_shopify_pnl_data',
    python_callable=load,
    pipeline_name='shopify-pnl',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=True,
    is_last_task=False,
    dag=dag
)

# load_view_task = TrackedPythonOperator(
#     task_id='shopify_sales_dashboard',
#     python_callable=load_view_pipeline,
#     pipeline_name='shopify-pnl',
#     client_id='bsc',
#     data_type='pnl-data',
#     is_first_task=False,
#     is_last_task=False,
#     dag=dag
# )



refresh_operational_pnl_order_details = TrackedPythonOperator(
    task_id='refresh_operational_pnl_order_details',
    python_callable=refresh_operational_pnl_order_details,
    pipeline_name='shopify-pnl',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    dag=dag
)

refresh_shopify_affiliate_validation = TrackedPythonOperator(
    task_id='refresh_shopify_affiliate_validation',
    python_callable=refresh_shopify_affiliate_validation,
    pipeline_name='shopify-pnl',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    dag=dag
)

refresh_shopify_discount_performance = TrackedPythonOperator(
    task_id='refresh_shopify_discount_performance',
    python_callable=refresh_shopify_discount_performance,
    pipeline_name='shopify-pnl',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    dag=dag
)

refresh_shopify_marketplace_summary = TrackedPythonOperator(
    task_id='refresh_shopify_marketplace_summary',
    python_callable=refresh_shopify_marketplace_summary,
    pipeline_name='shopify-pnl',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    dag=dag
)

refresh_shopify_sales_analysis_v2 = TrackedPythonOperator(
    task_id='refresh_shopify_sales_analysis_v2',
    python_callable=refresh_shopify_sales_analysis_v2,
    pipeline_name='shopify-pnl',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    dag=dag
)

refresh_shopify_pnl_combined = TrackedPythonOperator(
    task_id='refresh_shopify_pnl_combined',
    python_callable=refresh_shopify_pnl_combined,
    pipeline_name='shopify-pnl',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=True,
    dag=dag
)

end = EmptyOperator(task_id='end')

start >> load_task  >> refresh_operational_pnl_order_details >> refresh_shopify_affiliate_validation >> refresh_shopify_discount_performance >> refresh_shopify_marketplace_summary >> refresh_shopify_sales_analysis_v2 >> refresh_shopify_pnl_combined >> end
