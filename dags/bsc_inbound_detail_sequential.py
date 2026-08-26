from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow')

# Import task functions
from connections.bsc.vinculum.inbounddetail.extract import extract_data
from connections.bsc.vinculum.inbounddetail.transform import transform_data
from connections.bsc.vinculum.inbounddetail.load import load_data

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
    Get the next date range to process from a predefined list.
    Uses Airflow Variables to track progress.
    """
    # All the dates from your screenshot
    DATE_RANGES = [

        (datetime(2026, 6, 1), datetime(2026, 6, 17)),
    ]

    # Get current index from Variable (or start from beginning)
    try:
        current_index = int(Variable.get("vinculum_date_index"))
    except:
        # First run - start from index 0
        current_index = 0
        Variable.set("vinculum_date_index", str(current_index))

    # Check if we've processed all dates
    if current_index >= len(DATE_RANGES):
        print(f"All {len(DATE_RANGES)} date ranges processed. No more dates to process.")
        return None

    # Get the current date range
    start_date, end_date = DATE_RANGES[current_index]

    # Update the index for next run
    Variable.set("vinculum_date_index", str(current_index + 1))

    print(f"Processing date range {current_index + 1}/{len(DATE_RANGES)}: {start_date} to {end_date}")

    return {
        'start_date': start_date,
        'end_date': end_date
    }


def trigger_next_run(**context):
    """
    Determine if we should trigger the next run
    """
    try:
        current_index = int(Variable.get("vinculum_date_index"))

        # Total number of date ranges
        total_ranges = 1  # Update this if you add/remove dates

        if current_index >= total_ranges:
            print("All date ranges processed. Not triggering next run.")
            return None
        else:
            print(f"Next run will process date range {current_index + 1}/{total_ranges}")
            return "trigger_next"
    except:
        return None


# Create DAG
dag = DAG(
    dag_id='vinculum_inbound_detail_sequential',
    description='Sequential ETL pipeline for Vinculum Inbound Detail data (day by day)',
    schedule_interval=None,  # Manual trigger only
    default_args=default_args,
    start_date=datetime(2025, 3, 9),
    catchup=False,
    tags=['etl', 'vinculum', 'inbounddetail', 'sequential'],
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
    pipeline_name='vinculum-inbounddetail',
    client_id='bsc',
    data_type='order-details',
    is_first_task=True,
    dag=dag
)

# Transform task
transform_task = TrackedPythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    pipeline_name='vinculum-inbounddetail',
    client_id='bsc',
    data_type='order-details',
    is_last_task=False,
    dag=dag
)

# Load task
load_task = TrackedPythonOperator(
    task_id='load_data',
    python_callable=load_data,
    pipeline_name='vinculum-inbounddetail',
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
    trigger_dag_id='vinculum_inbound_detail_sequential',  # Self-trigger
    trigger_rule='none_failed',  # Only trigger if all previous tasks succeeded
    dag=dag
)

# Set dependencies
get_dates_task >> extract_task >> transform_task >> load_task >> check_next_run >> trigger_next