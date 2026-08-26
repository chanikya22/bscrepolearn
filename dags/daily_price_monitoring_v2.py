import sys
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

sys.path.append('/opt/airflow')

# Import task functions
from connections.insytscraping.flipkart.product_page_scraper import extract_data as flipkart_extract_data
from connections.insytscraping.blinkit.product_page_scraper import extract_data as blinkit_extract_data
from connections.insytscraping.zepto.product_page_scraper import extract_data as zepto_extract_data
from connections.insytscraping.swiggy.product_page_scraper import extract_data as swiggy_extract_data
from connections.insytscraping.amazon.product_page_scraper import extract_data as amazon_extract_data
from connections.insytscraping.bigbasket.product_page_scraper import extract_data as bigbasket_extract_data
from connections.insytscraping.myntra.product_page_scraper import extract_data as myntra_extract_data
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
# DAG 1: Daily Price Monitoring Extraction
# ========================================
extraction_dag = DAG(
    dag_id='bsc_daily_price_monitoring_v2',
    description='V2 - Daily Price Monitoring Extraction for BSC',
    schedule_interval='30 3 * * *',
    default_args=default_args,
    start_date=datetime(2025, 9, 9),
    catchup=False,
    tags=['price_monitor', 'monitor_prices', 'scraping', 'daily'],
)

# Define the extract task
blinkit_monitor_prices_daily = TrackedPythonOperator(
    task_id='monitor_prices_v2_blinkit',
    python_callable=blinkit_extract_data,
    op_kwargs={
         'connector': connector,
         'client_id': "bsc",
         'frequency': "Daily",
         'scheduled_hour': scheduled_hour,
    },
    pipeline_name='monitor_prices_v2',
    client_id='bsc',
    data_type='price_search',
    is_first_task=True,
    dag=extraction_dag
)

zepto_monitor_prices_daily = TrackedPythonOperator(
    task_id='monitor_prices_v2_zepto',
    python_callable=zepto_extract_data,
    op_kwargs={
         'connector': connector,
         'client_id': "bsc",
         'frequency': "Daily",
         'scheduled_hour': scheduled_hour,
    },
    pipeline_name='monitor_prices_v2',
    client_id='bsc',
    data_type='price_search',
    is_first_task=False,
    dag=extraction_dag
)



swiggy_monitor_prices_daily = TrackedPythonOperator(
    task_id='monitor_prices_v2_swiggy',
    python_callable=swiggy_extract_data,
    op_kwargs={
        'connector': connector,
        'client_id': "bsc",
        'frequency': "Daily",
        'scheduled_hour': scheduled_hour,
    },
    pipeline_name='monitor_prices_v2',
    client_id='bsc',
    data_type='price_search',
    is_first_task=False,
    dag=extraction_dag
)

bigbasket_monitor_prices_daily = TrackedPythonOperator(
    task_id='monitor_prices_v2_bigbasket',
    python_callable=bigbasket_extract_data,
    op_kwargs={
        'connector': connector,
        'client_id': "bsc",
        'frequency': "Daily",
        'scheduled_hour': scheduled_hour,
    },
    pipeline_name='monitor_prices_v2',
    client_id='bsc',
    data_type='price_search',
    is_first_task=False,
    dag=extraction_dag
)

flipkart_monitor_prices_daily = TrackedPythonOperator(
    task_id='monitor_prices_v2_flipkart',
    python_callable=flipkart_extract_data,
    op_kwargs={
        'connector': connector,
        'client_id': "bsc",
        'frequency': "Daily",
        'scheduled_hour': scheduled_hour,
    },
    pipeline_name='monitor_prices_v2',
    client_id='bsc',
    data_type='price_search',
    is_first_task=False,
    dag=extraction_dag
)

myntra_monitor_prices_daily = TrackedPythonOperator(
    task_id='monitor_prices_v2_myntra',
    python_callable=myntra_extract_data,
    op_kwargs={
        'connector': connector,
        'client_id': "bsc",
        'frequency': "Daily",
        'scheduled_hour': scheduled_hour,
    },
    pipeline_name='monitor_prices_v2',
    client_id='bsc',
    data_type='price_search',
    is_first_task=False,
    dag=extraction_dag
)


amazon_monitor_prices_daily = TrackedPythonOperator(
    task_id='monitor_prices_v2_amazon',
    python_callable=amazon_extract_data,
    op_kwargs={
        'connector': connector,
        'client_id': "bsc",
        'frequency': "Daily",
        'product_type':"Branded",
        'scheduled_hour': scheduled_hour,
    },
    pipeline_name='monitor_prices_v2',
    client_id='bsc',
    data_type='price_search',
    is_last_task=True,
    dag=extraction_dag
)


blinkit_monitor_prices_daily >> zepto_monitor_prices_daily >> swiggy_monitor_prices_daily >> bigbasket_monitor_prices_daily >> flipkart_monitor_prices_daily >> myntra_monitor_prices_daily >> amazon_monitor_prices_daily