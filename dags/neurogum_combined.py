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

sys.path.append('/opt/airflow')


#shopify_orders
from connections._100days.neurogum.shopify.orders.extract import extract_data
from connections._100days.neurogum.shopify.orders.transform import transform_data_to_postgres
from connections._100days.neurogum.shopify.orders.load import load_data

#google_ads
from connections._100days.neurogum.google_ads.extract import extract_data as extract_data_neurogum
from connections._100days.neurogum.google_ads.load import load_data as load_data_neurogum
from plugins.operators.tracked_python_operator import TrackedPythonOperator

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
        client_id='neurogum',
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

        # Tz-safe: normalize aware source_date to naive IST before subtract
        if getattr(source_date, 'tzinfo', None) is not None:
            source_date = source_date.astimezone(ist).replace(tzinfo=None)

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
        ('neurogum-shopify-orders', 'neurogum-shopify-orders'),
        ('google-neurogum-ads', 'google-neurogum-ads')
    ]

    date_ranges = {}
    for pipeline_name, data_type in pipelines:
        print(f"Pipeline Name: {pipeline_name}")
        print(f"Data Type: {data_type}")

        date_ranges[pipeline_name] = get_date_range_for_pipeline(pipeline_name, data_type)

    return date_ranges


# Create extract wrapper functions for each pipeline
def create_extract_wrapper(extract_func, pipeline_name,data_type):
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

# Default args

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False  # This prevents backfilling
}

dag = DAG(
    dag_id='neurogum_combined_pipeline',
    description='Neurogum Combined ETL',
    schedule_interval='30 7 * * *',  # Daily at 1 PM IST
    default_args=default_args,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'shopify', 'neurogum','google-ads'],
)


def run_shopify_orders_consolidated():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """
        CALL neurogum.shopify_orders_consolidated(
            (NOW() AT TIME ZONE 'Asia/Kolkata' - INTERVAL '3 months')::timestamp,
            (NOW() AT TIME ZONE 'Asia/Kolkata')::timestamp
        );
    """
    postgres.execute_query(query)


def run_shopify_sales_analysis():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """
        CALL neurogum.sp_shopify_sales_analysis();
    """
    postgres.execute_query(query)


run_shopify_orders_consolidated_task = TrackedPythonOperator(
    task_id='run_shopify_orders_consolidated',
    python_callable=run_shopify_orders_consolidated,
    pipeline_name='neurogum-shopify-orders',
    client_id='neurogum',
    data_type='neurogum-shopify-orders',
    is_first_task=False,
    is_last_task=False,
    dag=dag
)

run_shopify_sales_analysis_task = TrackedPythonOperator(
    task_id='run_shopify_sales_analysis',
    python_callable=run_shopify_sales_analysis,
    pipeline_name='neurogum-shopify-orders',
    client_id='neurogum',
    data_type='neurogum-shopify-orders',
    is_first_task=False,
    is_last_task=True,
    dag=dag
)

# --- DAG Structure ---
start = EmptyOperator(task_id='start',dag=dag)

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

shopify_extract = TrackedPythonOperator(
    task_id='neurogum_extract_from_source',
    python_callable=create_extract_wrapper(extract_data,"neurogum-shopify-orders",'neurogum-shopify-orders'),
    pipeline_name='neurogum-shopify-orders',
    client_id='neurogum',
    data_type='neurogum-shopify-orders',
    is_first_task=True,
    is_last_task=False,
    dag=dag
)

shopify_transform = TrackedPythonOperator(
    task_id='neurogum_transform_from_raw',
    python_callable=transform_data_to_postgres,
    pipeline_name='neurogum-shopify-orders',
    client_id='neurogum',
    data_type='neurogum-shopify-orders',
    is_first_task=False,
    is_last_task=False,
    dag=dag
)

shopify_load = TrackedPythonOperator(
    task_id='neurogum_load_to_iceberg',
    python_callable=load_data,
    pipeline_name='neurogum-shopify-orders',
    client_id='neurogum',
    data_type='neurogum-shopify-orders',
    is_first_task=False,
    is_last_task=False,
    dag=dag
)

shopify_complete = EmptyOperator(task_id='neurogum_shopify_complete',dag=dag)


# =============================================================================
# EXTRACT NEUROGUM GOOGLE ADS DATA
# =============================================================================

# Define the extract task
google_ads_extract_task = TrackedPythonOperator(
    task_id='neurogum_extract_google_ads_data',
    python_callable=create_extract_wrapper(extract_data_neurogum, 'google-neurogum-ads', 'google-neurogum-ads'),
    op_kwargs={
        'report_types': ['campaigns', 'keywords', 'ads']  # Extract all report types else mention specific ones
    },
    pipeline_name='google-neurogum-ads',
    client_id='neurogum',
    data_type='google-neurogum-ads',
    is_first_task=True,
    trigger_rule='none_failed',
    dag=dag
)

# Load task - loads the extracted data to database
google_ads_load_task = TrackedPythonOperator(
    task_id='neurogum_load_google_ads_data',
    python_callable=load_data_neurogum,
    pipeline_name='google-neurogum-ads',
    client_id='neurogum',
    data_type='google-neurogum-ads',
    is_last_task=True,
    trigger_rule='none_failed',
    dag=dag
)


end = EmptyOperator(
    task_id='end',
    dag=dag,
)
# flow
start >> check_previous_run_task >> get_all_dates_task
get_all_dates_task >>shopify_extract >> shopify_transform >> shopify_load >>run_shopify_orders_consolidated_task >> run_shopify_sales_analysis_task >>shopify_complete

# for google_ads
shopify_complete >> google_ads_extract_task >> google_ads_load_task >> end


