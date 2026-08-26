# dag2_sequential.py
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow')

# Import task functions
from connections.bsc.vinculum.invoicedetail_v2.extract import extract_data
from connections.bsc.vinculum.invoicedetail_v2.transform import transform_data
from connections.bsc.vinculum.invoicedetail_v2.load import load_data

# Import utilities
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from plugins.operators.tracked_python_operator import TrackedPythonOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'catchup': False
}

# def get_next_date_range(**context):
#     """
#     Get the next 6-hour date range to process.
#     Uses Airflow Variables to track progress.
#     """
#     # Configuration
#     START_DATE = datetime(2025, 5, 1)  # Your starting date and time
#     END_DATE = datetime(2025, 5, 27)   # Your ending date and time
#     INTERVAL_HOURS = 6  # 6-hour intervals

#     # Get current processing datetime from Variable (or start from beginning)
#     try:
#         current_datetime_str = Variable.get("vinculum_current_datetime")
#         current_datetime = datetime.strptime(current_datetime_str, "%Y-%m-%d %H:%M:%S")
#     except:
#         # First run - start from the beginning
#         current_datetime = START_DATE
#         Variable.set("vinculum_current_datetime", current_datetime.strftime("%Y-%m-%d %H:%M:%S"))
        
#         # Return the FIRST interval (00:00 to 06:00)
#         return {
#             'start_date': current_datetime,
#             'end_date': current_datetime + timedelta(hours=INTERVAL_HOURS)
#         }

#     # Calculate next 6-hour interval
#     next_datetime = current_datetime + timedelta(hours=INTERVAL_HOURS)

#     # Check if we've reached the end
#     if next_datetime >= END_DATE:
#         print(f"Reached end date {END_DATE}. No more intervals to process.")
#         return None

#     # Update the variable for next run
#     Variable.set("vinculum_current_datetime", next_datetime.strftime("%Y-%m-%d %H:%M:%S"))

#     return {
#         'start_date': next_datetime,
#         'end_date': next_datetime + timedelta(hours=INTERVAL_HOURS)
#     }


def get_next_date_range(**context):
    """
    Get the next date range to process.
    Uses Airflow Variables to track progress.
    """
    # Configuration
    START_DATE = datetime(2024, 4, 1)  # Your starting date
    END_DATE = datetime(2024, 5,1)  # Your ending date

    # Get current processing date from Variable (or start from beginning)
    try:
        current_date_str = Variable.get("vinculum_current_date")
        current_date = datetime.strptime(current_date_str, "%Y-%m-%d")
    except:
        # First run - start from the beginning
        current_date = START_DATE
        Variable.set("vinculum_current_date", current_date.strftime("%Y-%m-%d"))

    # Calculate next day
    next_date = current_date + timedelta(days=1)

    # Check if we've reached the end
    if next_date > END_DATE:
        print(f"Reached end date {END_DATE}. No more dates to process.")
        return None

    # Update the variable for next run
    Variable.set("vinculum_current_date", next_date.strftime("%Y-%m-%d"))

    return {
        'start_date': current_date,
        'end_date': next_date  # End of day
    }


def trigger_next_run(**context):
    """
    Determine if we should trigger the next run
    """
    try:
        current_date_str = Variable.get("vinculum_current_date")
        current_date = datetime.strptime(current_date_str, "%Y-%m-%d")
        
        if current_date >= datetime(2024, 5,1):
            print("All dates processed. Not triggering next run.")
            return None
        else:
            print(f"Next run will process starting from: {current_date}")
            return "trigger_next"
    except:
        return None


# Create DAG
dag = DAG(
    dag_id='vinculum_invoice_detail_v2_sequential',
    description='Sequential ETL pipeline for Vinculum Invoice Detail (V2) data (day by day)',
    schedule_interval=None,  # Manual trigger only
    default_args=default_args,
    start_date=datetime(2025, 3, 9),
    catchup=False,
    tags=['etl', 'vinculum', 'invoicedetail_v2', 'invoicedetail', 'sequential'],
)

# Get date range for current run
get_dates_task = PythonOperator(
    task_id='get_date_range',
    python_callable=get_next_date_range,
    dag=dag
)


# Extract task with dynamic dates
def extract_with_dynamic_dates(**context):
    date_range = context['task_instance'].xcom_pull(task_ids='get_date_range')
    if date_range is None:
        return "No dates to process"

    # Get run_id and tracker that TrackedPythonOperator provides
    run_id = context.get('run_id')
    tracker = context.get('tracker')

    return extract_data(
        start_date=date_range['start_date'],
        end_date=date_range['end_date'],
        run_id=run_id,
        tracker=tracker
    )


extract_task = TrackedPythonOperator(
    task_id='extract_data',
    python_callable=extract_with_dynamic_dates,
    pipeline_name='vinculum-invoicedetail',
    client_id='bsc',
    data_type='order-details',
    is_first_task=True,
    dag=dag
)

# Transform task
transform_task = TrackedPythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    pipeline_name='vinculum-invoicedetail',
    client_id='bsc',
    data_type='order-details',
    is_last_task=False,
    dag=dag
)

# Load task
load_task = TrackedPythonOperator(
    task_id='load_data',
    python_callable=load_data,
    pipeline_name='vinculum-invoicedetail',
    client_id='bsc',
    data_type='order-details',
    is_last_task=True,
    dag=dag
)

# Check if we should trigger next run
check_next_run = PythonOperator(
    task_id='check_next_run',
    python_callable=trigger_next_run,
    dag=dag
)

# Trigger next DAG run if there are more dates to process
trigger_next = TriggerDagRunOperator(
    task_id='trigger_next_run',
    trigger_dag_id='vinculum_invoice_detail_v2_sequential',  # Self-trigger
    trigger_rule='none_failed',  # Only trigger if all previous tasks succeeded
    dag=dag
)

# Set dependencies
get_dates_task >> extract_task >> transform_task >> load_task >> check_next_run >> trigger_next
