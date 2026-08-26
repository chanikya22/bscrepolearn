# dag2.py
from datetime import datetime, timedelta
import sys
import pandas as pd
from airflow.operators.python import PythonOperator
import pytz
from utils.pipeline_tracker import PipelineTracker
import os

sys.path.append('/opt/airflow')

# Import task functions
from connections.bsc.vinculum.returndetail.extract import extract_data
from connections.bsc.vinculum.returndetail.transform import transform_data
from connections.bsc.vinculum.returndetail.load import load_data

# Import utilities
from airflow import DAG
from airflow.models import Variable
from plugins.operators.tracked_python_operator import TrackedPythonOperator


def get_next_date_range(**context):
    """
    Get the next date range to process.
    Uses Airflow Variables to track progress.
    """
    tracker = PipelineTracker(db_prefix="audit_")

    result = tracker.get_data_registry(
        client_id='bsc',
        data_type='vinculum-returndetail',
        is_current= 'true',
        limit=1,
        sort_by='run_id',
        sort_order='DESC'
    )


    # Extract the source_updated_at value
    if not result.empty:
        source_updated_at = result['source_updated_at'].iloc[0]
        print(f"Latest source_updated_at: {source_updated_at}")

        # Convert pandas Timestamp to Python datetime if needed
        if isinstance(source_updated_at, pd.Timestamp):
            source_date = source_updated_at.to_pydatetime()
        else:
            source_date = source_updated_at
        
        ist = pytz.timezone('Asia/Kolkata')
        current_time_ist = datetime.now(ist).replace(tzinfo=None)
        time_diff = current_time_ist - source_date

        if time_diff.total_seconds() < 300:  # Less than 5 min ago
            print(f"Data is already up-to-date. Source updated at: {source_date}, Current time: {current_time_ist}")
            START_DATE = source_date
            END_DATE = source_date
            message = "Data is already up-to-date"
        else:
            print(f"Data needs update. Source updated at: {source_date}, Current time: {current_time_ist}")
            START_DATE = source_date
            END_DATE = current_time_ist
            message = "Processing next date range"
    else:
        print("No records found matching the criteria")
        START_DATE = datetime(2025, 6, 5, 0)
        END_DATE = datetime(2025, 6, 6, 0)
        message = "Using default date range"

    # Convert to ISO format strings for JSON serialization
    return {
        'start_date': START_DATE.isoformat(),
        'end_date': END_DATE.isoformat(),
        'message': message
    }

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

# Create DAG directly
dag = DAG(
    dag_id='vinculum_return_detail',
    description='ETL pipeline for Vinculum Return Detail data',
    schedule_interval='45 * * * *',  # Every hour at minute 45
    default_args=default_args,
    start_date=datetime(2025, 3, 9),
    catchup=False,
    tags=['etl', 'vinculum', 'returndetail', 'return'],
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
        print("No dates to process - pipeline complete")
        return "No dates to process"

    # Convert ISO format strings back to datetime objects
    start_date = datetime.fromisoformat(date_range['start_date'])
    end_date = datetime.fromisoformat(date_range['end_date'])

    print(f"Processing data from {start_date} to {end_date}")

    # Get run_id and tracker that TrackedPythonOperator should provide
    run_id = context.get('run_id')
    tracker = context.get('tracker')

    # Create a copy of context and add our specific parameters
    extract_context = context.copy()
    extract_context.update({
        'start_date': start_date,
        'end_date': end_date,
        'run_id': run_id,
        'tracker': tracker
    })

    # Call extract_data with the enhanced context
    return extract_data(**extract_context)

# Define the extract task
extract_task = TrackedPythonOperator(
    task_id='extract_data',
    python_callable=extract_with_dynamic_dates,
    pipeline_name='vinculum-returndetail',
    client_id='bsc',
    data_type='order-details',
    is_first_task=True,
    dag=dag
)

# Transform task
transform_task = TrackedPythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    pipeline_name='vinculum-returndetail',
    client_id='bsc',
    data_type='order-details',
    is_last_task=False,
    dag=dag
)

# Load task
load_task = TrackedPythonOperator(
    task_id='load_data',
    python_callable=load_data,
    pipeline_name='vinculum-returndetail',
    client_id='bsc',
    data_type='order-details',
    is_last_task=True,
    dag=dag
)

# Set dependencies
get_dates_task >> extract_task >> transform_task >> load_task