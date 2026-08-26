import sys
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

sys.path.append('/opt/airflow')

# Import task functions
from connections.insytscraping.amazon.product_page_scraper import extract_data as amazon_extract_data
from utils.postgresconnector_v3 import PostgresConnector
from utils.pipeline_tracker import PipelineTracker

# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

# Initialize connector and tracker
connector = PostgresConnector()
tracker = PipelineTracker(db_prefix="audit_")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_scheduled_hour() -> str:
    now = datetime.now(IST)
    return now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

scheduled_hour = get_scheduled_hour()  # computed once when DAG is parsed

# ========================================
# DAG 1: BiWeekly Price Monitoring Extraction
# ========================================
extraction_dag = DAG(
    dag_id='bsc_competition_price_monitoring',
    description='Competition Price Monitoring Extraction From Amazon',
    schedule_interval='0 6 * * 2,5',  # Tuesdays & Fridays at 12pm IST (06:00 UTC)
    default_args=default_args,
    start_date=datetime(2025, 9, 9),
    catchup=False,
    tags=['price_monitor', 'monitor_prices', 'scraping', 'competition'],
)

# Define the extract task
amazon_monitor_prices_daily = TrackedPythonOperator(
    task_id='monitor_prices_v2_amazon',
    python_callable=amazon_extract_data,
    op_kwargs={
        'connector': connector,
        'client_id': "bsc",
        'frequency': "Daily",
        'product_type':"Competition",
        'scheduled_hour': scheduled_hour,
    },
    pipeline_name='monitor_prices_v2',
    client_id='bsc',
    data_type='price_search',
    is_first_task=True,
    is_last_task=True,
    dag=extraction_dag
)


amazon_monitor_prices_daily