from datetime import datetime, timedelta
import sys
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State
from airflow.models import DagRun
import pytz
from utils.pipeline_tracker import PipelineTracker
import os

from utils.postgresconnector_v3 import PostgresConnector

sys.path.append('/opt/airflow')

# Import task functions for pipelines

# GoKwik Shipment BSC Orders Pipeline Tasks
from connections.bsc.gokwik.shipment.bsc.extract import extract_data as bsc_extract_data
from connections.bsc.gokwik.shipment.bsc.transform import transform_data as bsc_transform_data
from connections.bsc.gokwik.shipment.bsc.load import load_data as bsc_load_data


# GoKwik Shipment BAE Orders Pipeline Tasks
from connections.bsc.gokwik.shipment.bombae.extract import extract_data as bae_extract_data
from connections.bsc.gokwik.shipment.bombae.transform import transform_data as bae_transform_data
from connections.bsc.gokwik.shipment.bombae.load import load_data as bae_load_data

# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com'
]


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
            print(f"Data is already up-to-date for {pipeline_name}. Source updated at: {source_date}, Current time: {current_time_ist}")
            START_DATE = source_date
            END_DATE = source_date
            message = f"Data is already up-to-date for {pipeline_name}"
        else:
            print(f"Data needs update for {pipeline_name}. Source updated at: {source_date}, Current time: {current_time_ist}")
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
        ('bsc-gokwik-shipment', 'bsc-gokwik-shipment'),
        ('bae-gokwik-shipment', 'bae-gokwik-shipment')
    ]
    
    date_ranges = {}
    for pipeline_name, data_type in pipelines:
        print(f"Pipeline Name: {pipeline_name}")
        print(f"Data Type: {data_type}")

        date_ranges[pipeline_name] = get_date_range_for_pipeline(pipeline_name, data_type)
    
    return date_ranges


# Create extract wrapper functions for each pipeline
def create_extract_wrapper(extract_func, pipeline_name, data_type):
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
            'tracker': tracker
        })

        # Call extract_data with the enhanced context
        return extract_func(**extract_context)
    
    return extract_with_dynamic_dates


# Default arguments - MODIFIED TO PREVENT OVERLAPPING RUNS
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False  # This prevents backfilling
}


# Create DAG - MODIFIED TO PREVENT OVERLAPPING RUNS
dag = DAG(
    dag_id='bsc_gokwik_shipment_orders',
    description='ETL pipeline for all Shopify orders',
    schedule_interval='30 0 * * *',  # Daily at 6 AM IST
    default_args=default_args,
    start_date=datetime(2025, 3, 9),
    catchup=False,
    max_active_runs=1,  # CRITICAL: Only allow 1 active run at a time
    max_active_tasks=10,  # Limit concurrent tasks within a DAG run
    tags=['shopify', 'gokwik', 'bsc'],
) 
start = EmptyOperator(
    task_id='start',
    dag=dag,
)


# ADDED: Check if previous DAG run is complete
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

# Start marker
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# End marker  
end_task = DummyOperator(
    task_id='end_pipeline',
    trigger_rule='none_failed_min_one_success',  # Continue even if some tasks fails
    dag=dag
)


# # =============================================================================
# # O2S OSD O2A O2P O2Packed Dashboard Refresh
# # =============================================================================

def refresh_shopify_order_to_deliver_summary():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = "CALL bsc.refresh_shopify_order_to_deliver_summary();"
    postgres.execute_query(query)

def refresh_shopify_order_to_allocation_summary():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = "CALL bsc.refresh_shopify_order_to_allocation_summary();"
    postgres.execute_query(query)

def refresh_shopify_order_to_pick_summary():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = "CALL bsc.refresh_shopify_order_to_pick_summary();"
    postgres.execute_query(query)

def refresh_shopify_order_to_pack_summary():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = "CALL bsc.refresh_shopify_order_to_pack_summary();"
    postgres.execute_query(query)

# # =============================================================================
# # LOAD GOKWIK BSC ORDERS PIPELINE
# # =============================================================================

# extract order tasks
bsc_gokwik_order_extract_task = TrackedPythonOperator(
    task_id='bsc_gokwik_order_extract_data',
    python_callable=create_extract_wrapper(bsc_extract_data, 'bsc-gokwik-shipment', 'bsc-gokwik-shipment'),
    pipeline_name='bsc-gokwik-shipment',
    client_id='bsc',
    data_type='bsc-gokwik-shipment',
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',  # Run if upstream didn't fail
    dag=dag
)

# transform orders
bsc_gokwik_order_transform_task = TrackedPythonOperator(
    task_id='bsc_gokwik_order_transform_data',
    python_callable=bsc_transform_data,
    pipeline_name='bsc-gokwik-shipment',
    client_id='bsc',
    data_type='bsc-gokwik-shipment',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

# load orders
bsc_gokwik_order_load_task = TrackedPythonOperator(
    task_id='bsc_gokwik_order_load_data',
    python_callable=bsc_load_data,
    pipeline_name='bsc-gokwik-shipment',
    client_id='bsc',
    data_type='bsc-gokwik-shipment',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

# bsc order pipeline completion marker
bsc_gokwik_pipeline_complete = DummyOperator(
    task_id='bsc_gokwik_pipeline_complete',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

# # =============================================================================
# LOAD GOKWIK BAE ORDERS PIPELINE
# # =============================================================================

# extract order tasks
bae_gokwik_order_extract_task = TrackedPythonOperator(
    task_id='bae_gokwik_order_extract_data',
    python_callable=create_extract_wrapper(bae_extract_data, 'bae-gokwik-shipment', 'bae-gokwik-shipment'),
    pipeline_name='bae-gokwik-shipment',
    client_id='bsc',
    data_type='bae-gokwik-shipment',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',  # Run if upstream didn't fail
    dag=dag
)

# transform orders
bae_gokwik_order_transform_task = TrackedPythonOperator(
    task_id='bae_gokwik_order_transform_data',
    python_callable=bsc_transform_data,
    pipeline_name='bae-gokwik-shipment',
    client_id='bsc',
    data_type='bae-gokwik-shipment',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

# load orders
bae_gokwik_order_load_task = TrackedPythonOperator(
    task_id='bae_gokwik_order_load_data',
    python_callable=bsc_load_data,
    pipeline_name='bae-gokwik-shipment',
    client_id='bsc',
    data_type='bae-gokwik-shipment',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

# bae order pipeline completion marker
bae_gokwik_pipeline_complete = DummyOperator(
    task_id='bae_gokwik_pipeline_complete',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)


# # =============================================================================
# # O2S OSD O2A O2P O2Packed Dashboard Refresh
# # =============================================================================

refresh_allocation_task = PythonOperator(
    task_id='refresh_shopify_order_to_allocation_summary',
    python_callable=refresh_shopify_order_to_allocation_summary,
    dag=dag
)

refresh_pick_task = PythonOperator(
    task_id='refresh_shopify_order_to_pick_summary',
    python_callable=refresh_shopify_order_to_pick_summary,
    dag=dag
)

refresh_pack_task = PythonOperator(
    task_id='refresh_shopify_order_to_pack_summary',
    python_callable=refresh_shopify_order_to_pack_summary,
    dag=dag
)

refresh_deliver_task = PythonOperator(
    task_id='refresh_shopify_order_to_deliver_summary',
    python_callable=refresh_shopify_order_to_deliver_summary,
    dag=dag
)


# Initial setup with overlap prevention
start_task >> check_previous_run_task >> get_all_dates_task 

get_all_dates_task >> bae_gokwik_order_extract_task >> bae_gokwik_order_transform_task >> bae_gokwik_order_load_task >> bae_gokwik_pipeline_complete

bae_gokwik_pipeline_complete >> bsc_gokwik_order_extract_task >> bsc_gokwik_order_transform_task >> bsc_gokwik_order_load_task >> bsc_gokwik_pipeline_complete

bsc_gokwik_pipeline_complete >> refresh_allocation_task >> refresh_pick_task >> refresh_pack_task >> refresh_deliver_task >> end_task