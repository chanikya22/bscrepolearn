from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')

# Import task functions
from connections.price_monitoring.extract import monitor_prices
from connections.bsc.whatsApp.price_monitoring.message import send_price_comparison_report_whatsapp
from connections.bsc.whatsApp.price_alert.extract import send_alert
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

# ========================================
# DAG 1: Hourly Price Monitoring Extraction
# ========================================
extraction_dag = DAG(
    dag_id='bsc_monitor_prices_extraction_hourly',
    description='Hourly Price Monitoring Extraction for BSC',
    schedule_interval='0 */2 * * *',    # Run every hour at minute 0
    default_args=default_args,
    start_date=datetime(2025, 9, 9),
    catchup=False,
    tags=['price_monitor', 'monitor_prices', 'scraping', 'hourly'],
)

# Define the extract task
monitor_prices_hourly = TrackedPythonOperator(
    task_id='monitor_prices',
    python_callable=monitor_prices,
    op_kwargs={
         'connector': connector,
         'extraction_api_url': "http://3.7.121.98:5001/api/Extract",
         'client_id': "bsc",
         'batch_size': 50,
         'max_workers': 10,
         'rate_limit': 300
    },
    pipeline_name='monitor_prices',
    client_id='bsc',
    data_type='price_search',
    is_first_task=True,
    dag=extraction_dag
)
send_alert_trimmer = TrackedPythonOperator(
    task_id='send_alert_trimmer',
    python_callable=send_alert,
    op_kwargs={
        'category': ("TRIMMER", "HAIR STYLING"),
        'send_to': [
                     '918171366828',                #Shishank
                     '120363029450897434@g.us',     #Ecomteam
                     #'919878945424'                 #Mayank
                 ]
    },
    pipeline_name='monitor_prices',
    client_id='bsc',
    data_type='price_alert',
    is_first_task=False,
    dag=extraction_dag
)
#send_alert_fragrances = TrackedPythonOperator(
#    task_id='send_alert_fragrances',
#    python_callable=send_alert,
#    op_kwargs={
#        'category': ("FRAGRANCES",),
#        'send_to': [
#                     '918171366828',                #Shishank
#                     '120363029450897434@g.us',     #Ecomteam
#                     '919878945424'                 #Mayank
#                 ]
#    },
#    pipeline_name='monitor_prices',
#    client_id='bsc',
#    data_type='price_alert',
#    is_last_task=True,
#    dag=extraction_dag
#)

monitor_prices_hourly >> send_alert_trimmer #>> send_alert_fragrances
# ========================================
# DAG 2: Daily WhatsApp Reporting at 4:30 AM
# ========================================
reporting_dag = DAG(
    dag_id='bsc_monitor_prices_reporting_daily',
    description='Daily WhatsApp Price Monitoring Reports for BSC at 4:30 AM',
    schedule_interval='30 4 * * *',  # Run daily at 4:30 AM
    default_args=default_args,
    start_date=datetime(2025, 9, 9),
    catchup=False,
    tags=['price_monitor', 'monitor_prices', 'whatsapp', 'daily', 'reporting'],
)

# Send trimmer message task
send_trimmer_message = TrackedPythonOperator(
    task_id='send_trimmer_message',
    python_callable=send_price_comparison_report_whatsapp,
    op_kwargs={
         'category': ("TRIMMER", "HAIR STYLING"),
         'send_to': [
             '120363191112784778@g.us',  # Group 1
             '120363399780100427@g.us',  # Group 2
             '917003540396',              # Varun
             '919910084505',              # DG
             '919834814856',               # Avani
              '120363029450897434@g.us' ,    #Ecomteam
             '919833586983'               # Ashu

         ]
    },
    pipeline_name='monitor_prices',
    client_id='bsc',
    data_type='price_search',
    is_first_task=True,
    dag=reporting_dag
)

# Send fragrances message task
send_fragrances_message = TrackedPythonOperator(
    task_id='send_fragrances_message',
    python_callable=send_price_comparison_report_whatsapp,
    op_kwargs={
         'category': ("FRAGRANCES",),
         'send_to': [
             '919891361147',              # Contact 1
             '918697701791',              # Contact 2
              '120363029450897434@g.us',     #Ecomteam
             '120363399780100427@g.us'   # Group
         ]
    },
    pipeline_name='monitor_prices',
    client_id='bsc',
    data_type='price_search',
    is_last_task=True,
    dag=reporting_dag
)
# Send skincare message task
send_skincare_message = TrackedPythonOperator(
    task_id='send_skincare_message',
    python_callable=send_price_comparison_report_whatsapp,
    op_kwargs={
         'category': ("SKINCARE",),
         'send_to': [
             '918171366828',              # Contact 1
             '919891361147',              # RT
              '919910414625'     #Eshan
             #'120363399780100427@g.us'   # Group
         ]
    },
    pipeline_name='monitor_prices',
    client_id='bsc',
    data_type='price_search',
    is_last_task=True,
    dag=reporting_dag
)
# Set dependencies for reporting DAG only
send_trimmer_message >> send_fragrances_message >>send_skincare_message