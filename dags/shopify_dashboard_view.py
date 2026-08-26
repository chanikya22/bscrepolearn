from datetime import datetime, timedelta
import sys
import pandas as pd
import logging
from airflow.operators.empty import EmptyOperator
sys.path.append('/opt/airflow')
# Import task functions
from connections.bsc.shopify.shopify_pnl.load import  create_view_shopify_pnl_combined , create_view_shopify_sales_analysisV2
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
    dag_id='shopify_dashboard_views',
    description='ETL pipeline for Shopify PnL data for BSC',
    schedule_interval='0 0 * * *',  # Daily at 6 AM IST
    default_args=default_args,
    start_date=datetime(2025, 7, 9),
    catchup=False,
    tags=['etl', 'shopify', 'pnl'],
)


start = EmptyOperator(task_id='start')
# Define the load task


# load_view = TrackedPythonOperator(
#     task_id='load_view_pipeline',
#     python_callable=load_view_pipeline,
#     pipeline_name='shopify-pnl',
#     client_id='bsc',
#     data_type='pnl-data',
#     is_first_task=True,
#     is_last_task=False,
#     dag=dag
# )

# Create task for shopify_sales_analysisV2
create_sales_analysis = TrackedPythonOperator(
    task_id='create_view_shopify_sales_analysisV2',
    python_callable=create_view_shopify_sales_analysisV2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=True,
    is_last_task=False,
    dag=dag
)


create_pnl_combined = TrackedPythonOperator(
    task_id='create_view_shopify_pnl_combined2',
    python_callable=create_view_shopify_pnl_combined,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=True,
    dag=dag
)
    

end = EmptyOperator(task_id='end')

start >> create_sales_analysis >> create_pnl_combined >> end
