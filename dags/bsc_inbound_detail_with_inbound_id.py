# Import utilities
from datetime import datetime, timedelta
import sys
from airflow import DAG
import pandas as pd
from utils.postgresconnector_v3 import PostgresConnector
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State
from airflow.models import DagRun
import pytz
from utils.pipeline_tracker import PipelineTracker
from plugins.operators.tracked_python_operator import TrackedPythonOperator



sys.path.append('/opt/airflow')

# Import task functions
from connections.bsc.vinculum.inbounddetail.extract import extract_data_with_inbound_id
from connections.bsc.vinculum.inbounddetail.transform import transform_data
from connections.bsc.vinculum.inbounddetail.load import load_data


# ======================================================
# Prevent overlapping DAG runs
# ======================================================
def check_previous_dag_completion(**context):
    """
    Check if the previous DAG run has completed (either success or failed).
    If still running, skip this DAG run.
    """
    from airflow.models import DagRun
    from airflow.utils.db import provide_session

    dag_id = context['dag'].dag_id
    current_execution_date = context['execution_date']

    @provide_session
    def get_previous_dag_runs(session=None):
        # Get all previous DAG runs that are still running
        running_dag_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date < current_execution_date,
            DagRun.state.in_([State.RUNNING, State.QUEUED])
        ).all()

        return running_dag_runs

    running_runs = get_previous_dag_runs()

    if running_runs:
        print(f"Found {len(running_runs)} previous DAG runs still running:")
        for run in running_runs:
            print(f"  - Run ID: {run.run_id}, Execution Date: {run.execution_date}, State: {run.state}")

        # Skip this DAG run by raising an exception that will mark the task as skipped
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("Previous DAG run is still running. Skipping this run.")

    print("No previous DAG runs are currently running. Proceeding with this run.")
    return "proceed"


def get_date_range_for_pipeline(pipeline_name, data_type):
    """
    Get the date range for a specific pipeline.
    """
    tracker = PipelineTracker(db_prefix="audit_")

    print(f"Pipeline name: {pipeline_name}")
    print(f"Data type: {data_type}")

    result = tracker.get_data_registry(
        client_id='bsc',
        data_type=data_type,
        is_current='true',
        limit=1,
        sort_by='run_id',
        sort_order='DESC'
    )

    if not result.empty:
        source_updated_at = result['source_updated_at'].iloc[0]
        print(f"Latest source_updated_at for {pipeline_name}: {source_updated_at}")

        # Convert pandas Timestamp to Python datetime if needed
        if isinstance(source_updated_at, pd.Timestamp):
            source_date = source_updated_at.to_pydatetime()
        else:
            source_date = source_updated_at

        ist = pytz.timezone('Asia/Kolkata')
        current_time_ist = datetime.now(ist).replace(tzinfo=None)

        time_diff = current_time_ist - source_date

        if time_diff.total_seconds() < 300:  # Less than 5 min ago
            print(
                f"Data is already up-to-date for {pipeline_name}. Source updated at: {source_date}, Current time: {current_time_ist}")
            START_DATE = source_date
            END_DATE = source_date
            message = f"Data is already up-to-date for {pipeline_name}"
        else:
            print(
                f"Data needs update for {pipeline_name}. Source updated at: {source_date}, Current time: {current_time_ist}")
            START_DATE = source_date
            END_DATE = current_time_ist
            message = f"Processing next date range for {pipeline_name}"

        print(f"START_DATE for {pipeline_name}: {START_DATE}")
        print(f"END_DATE for {pipeline_name}: {END_DATE}")
    else:
        print(f"No records found for {pipeline_name}")
        START_DATE = datetime(2025, 11, 4, 0)
        END_DATE = datetime(2025, 11, 5, 0)
        message = f"Using default date range for {pipeline_name}"

    return {
        'start_date': START_DATE.isoformat(),
        'end_date': END_DATE.isoformat(),
        'message': message
    }


def get_all_date_ranges(**context):
    """
    Get date ranges for all pipelines.
    pipelines : pipeline_name, data_type
    """
    pipelines = [
        ('vinculum_inbounddetail_inbound_id', 'vinculum-inbounddetail')
    ]

    date_ranges = {}
    for pipeline_name, data_type in pipelines:
        print(f"Pipeline Name: {pipeline_name}")
        print(f"Data Type: {data_type}")

        date_ranges[pipeline_name] = get_date_range_for_pipeline(pipeline_name, data_type)

    return date_ranges


# Create extract wrapper functions for each pipeline
def create_extract_wrapper(extract_func, pipeline_name,store_name):
    def extract_with_dynamic_dates(**context):
        # Get all date ranges
        all_date_ranges = context['task_instance'].xcom_pull(task_ids='get_all_date_ranges')
        if all_date_ranges is None or pipeline_name not in all_date_ranges:
            print(f"No dates to process for {pipeline_name} - pipeline complete")
            return f"No dates to process for {pipeline_name}"

        date_range = all_date_ranges[pipeline_name]

        # Convert ISO format strings back to datetime objects
        start_date = datetime.fromisoformat(date_range['start_date'])
        end_date = datetime.fromisoformat(date_range['end_date'])

        print(f"Processing {pipeline_name} data from {start_date} to {end_date}")

        # Get run_id and tracker that TrackedPythonOperator should provide
        run_id = context.get('run_id')
        tracker = context.get('tracker')

        # Create a copy of context and add our specific parameters
        extract_context = context.copy()
        extract_context.update({
            'start_date': start_date,
            'end_date': end_date,
            'run_id': run_id,
            'tracker': tracker,
            'store': store_name
        })

        # Call extract_data with the enhanced context
        return extract_func(**extract_context)

    return extract_with_dynamic_dates

# Default args

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



# Create DAG
dag = DAG(
    dag_id='vinculum_inbound_detail_with_inbound_id',
    description='Sequential ETL pipeline for Vinculum Inbound Detail data by inbound_id',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    default_args=default_args,
    start_date=datetime(2025, 3, 9),
    catchup=False,
    tags=['etl', 'vinculum', 'inbounddetail', 'sequential'],
)


start = EmptyOperator(task_id='start')

check_previous_run_task = PythonOperator(
    task_id='check_previous_dag_completion',
    python_callable=check_previous_dag_completion,
    dag=dag
)

# Get date ranges for all pipelines
get_all_dates_task = PythonOperator(
    task_id='get_all_date_ranges',
    python_callable=get_all_date_ranges,
    dag=dag
)


extract_task = TrackedPythonOperator(
    task_id='extract_data',
    python_callable=create_extract_wrapper(extract_data_with_inbound_id, 'vinculum_inbounddetail_inbound_id', 'vinculum-inbounddetail'),    
    pipeline_name='vinculum_inbounddetail_inbound_id',
    client_id='bsc',
    data_type='vinculum-inbounddetail',
    is_first_task=True,
    dag=dag
)

# Transform task
transform_task = TrackedPythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    pipeline_name='vinculum_inbounddetail_inbound_id',
    client_id='bsc',
    data_type='vinculum-inbounddetail',
    is_last_task=False,
    dag=dag
)

# Load task
load_task = TrackedPythonOperator(
    task_id='load_data',
    python_callable=load_data,
    pipeline_name='vinculum_inbounddetail_inbound_id',
    client_id='bsc',
    data_type='vinculum-inbounddetail',
    is_last_task=True,
    dag=dag
)

end_task = EmptyOperator(
    task_id="end_pipeline",
    dag=dag
)
# Set dependencies

start >> check_previous_run_task >> get_all_dates_task >> extract_task >> transform_task >> load_task >> end_task
