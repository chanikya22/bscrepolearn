from datetime import datetime, timedelta
import sys
from airflow.operators.python import PythonOperator

sys.path.append('/opt/airflow')
# Import utilities
from airflow import DAG
from connections.insytscraping.channel_pricing.extract import get_prices
from utils.postgresconnector_v3 import PostgresConnector
from plugins.operators.tracked_python_operator import TrackedPythonOperator

# Initialize connector and tracker
connector = PostgresConnector()

ALERT_EMAILS = [
    'tech@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com'
    'manish@bombayshavingcompany.com',

]

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG directly
dag = DAG(
    dag_id='Channel_pricing_scraper',
    description='pipeline to extract price and load in database',
    schedule_interval='3 1 * * *',  # Daily at 6:33 AM (IST)
    default_args=default_args,
    start_date=datetime(2026, 1, 12),
    catchup=False,
    tags=['gobblecube', 'scraping', 'sp', 'bsc'],
)

# Define the extract task - this might need start_date and end_date params
blinkit_price = TrackedPythonOperator(
    task_id='price_report_blinkit',
    python_callable=get_prices,
    op_kwargs={
        'platform': 'Blinkit',
        'connector': connector,
        'extraction_api_url': "http://3.7.121.98:5001/api/Extract",
        'client_id': "bsc"},
    failure_email_to=ALERT_EMAILS,
    dag=dag)
instamart_price = TrackedPythonOperator(
    task_id='price_report_instamart',
    python_callable=get_prices,
    op_kwargs={
        'platform': 'Swiggy',
        'connector': connector,
        'extraction_api_url': "http://3.7.121.98:5001/api/Extract",
        'client_id': "bsc"},
    failure_email_to=ALERT_EMAILS,
    dag=dag)
zepto_price = TrackedPythonOperator(
    task_id='price_report_zepto',
    python_callable=get_prices,
    op_kwargs={
        'platform': 'Zepto',
        'connector': connector,
        'extraction_api_url': "http://3.7.121.98:5001/api/Extract",
        'client_id': "bsc"},
    failure_email_to=ALERT_EMAILS,
    dag=dag)
amazon_price = TrackedPythonOperator(
    task_id='price_report_amazon',
    python_callable=get_prices,
    op_kwargs={
        'platform': 'Amazon',
        'connector': connector,
        'extraction_api_url': "http://3.7.121.98:5001/api/Extract",
        'client_id': "bsc"},
    failure_email_to=ALERT_EMAILS,
    dag=dag)

blinkit_price >> instamart_price >> zepto_price >> amazon_price
