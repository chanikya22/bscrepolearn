from datetime import datetime, timedelta
import sys
import pandas as pd
from utils.postgresconnector_v3 import PostgresConnector
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator
from utils.airbyte import run_airbyte_connection
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import State
from airflow.models import DagRun
import pytz
from utils.pipeline_tracker import PipelineTracker
import os

from utils.postgresconnector import PostgresConnector

sys.path.append('/opt/airflow')

# Import task functions for pipelines

# Freshdesk BSC functions
from connections.bsc.freshdesk.extract import extract_data as freshdesk_extract
from connections.bsc.freshdesk.transform_ticket import transform_tickets as freshdesk_transform_ticket
from connections.bsc.freshdesk.transform_conversation import transform_conversations as freshdesk_transform_conversation
from connections.bsc.freshdesk.load import load_data as freshdesk_load



# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'ayushgoyal@bombayshavingcompany.com',
    'lakshay@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',
    
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
        START_DATE = datetime(2026, 1, 1, 0)
        END_DATE = datetime(2026, 12, 31, 0)
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
        
        ('bsc-freshdesk', 'bsc-freshdesk')
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

# # =============================================================================
# AIRBYTE SYNC FUNCTION
# # =============================================================================

def trigger_bsc_freshdesk(**context):
    connection_id = "e63870c7-d99c-4d45-bbad-cb48eb06a940"  
    return run_airbyte_connection(connection_id)


# # =============================================================================
# DASHBOARD REFRESH FUNCTIONS
# # =============================================================================

def create_view_freshdesk_agent_map():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """CREATE OR REPLACE VIEW bsc.v_agent_team_map AS
                SELECT
                    a.id                        AS agent_id,
                    contact->>'name'            AS agent_name,
                    contact->>'email'           AS agent_email,
                    -- Team name: pulled from the 'group_memberships' or a team field in base_agents.
                    -- Adjust the jsonb path below to match your actual schema.
                    -- Common patterns: contact->>'department', custom_fields->>'team', etc.
                    CASE
                        WHEN contact->>'email' LIKE '%bombayshaving%' THEN 'BSC'
                        WHEN contact->>'email' LIKE '%maxicus%'       THEN 'MAXICUS'
                        END                           AS team_name,
                    CASE
                        WHEN contact->>'email' LIKE '%bombayshaving%' THEN 'BSC'
                        WHEN contact->>'email' LIKE '%maxicus%'       THEN 'MAXICUS'
                        END                         AS support_type
                FROM freshdesk.base_agents a
                WHERE (contact->>'email' LIKE '%bombayshaving%'
                    OR contact->>'email' LIKE '%maxicus%');"""
    postgres.execute_query(query)

def refresh_freshdesk_stored_procedures():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """BEGIN ;
                CALL bsc.refresh_agent_fcr_metrics();
                CALL bsc.refresh_agent_satisfaction_ratings();
                CALL bsc.refresh_agent_ranking_monthly();
                COMMIT;"""
    postgres.execute_query(query)

def refresh_freshdesk_dash():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """CREATE OR REPLACE VIEW bsc.v_agent_ranking_dashboard AS
                SELECT
                    month_year,
                    month_label,
                    final_rank,
                    agent_name,
                    team_name,
                    support_type,

                    -- Formatted percentages (multiply by 100 for display)
                    ROUND(resolution_pct * 100, 2)  AS resolution_pct_display,
                    frt_display                      AS first_response_time,
                    ROUND(fcr_pct * 100, 2)         AS fcr_pct_display,
                    ROUND(csat_pct * 100, 2)        AS csat_pct_display,

                    -- Component ranks
                    resolution_rank,
                    frt_rank,
                    csat_rank,
                    fcr_rank,

                    -- Raw numbers (useful for drill-down)
                    total_tickets,
                    resolved_tickets,
                    avg_frt_seconds,
                    fcr_eligible,
                    fcr_tickets,
                    csat_positive,
                    csat_negative,
                    csat_neutral,

                    weighted_score,
                    refreshed_at
                FROM bsc.agent_ranking_monthly
                ORDER BY month_year DESC, final_rank ASC;"""
    postgres.execute_query(query)


# Default arguments - MODIFIED TO PREVENT OVERLAPPING RUNS
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 6,
    'retry_delay': timedelta(minutes=1),
    'catchup': False  # This prevents backfilling
}


# Create DAG - MODIFIED TO PREVENT OVERLAPPING RUNS
dag = DAG(
    dag_id='bsc_freshdesk_pipeline',
    description='End to end  pipeline for all Shopify freshdesk data with Dashboard Views',
    schedule_interval='30 23 * * *',  # Daily at 5:00 AM AND 5:00 PM IST
    default_args=default_args,
    start_date=datetime(2025, 3, 9),
    catchup=False,
    max_active_runs=1,  # CRITICAL: Only allow 1 active run at a time
    max_active_tasks=10,  # Limit concurrent tasks within a DAG run
    tags=['shopify', 'bsc', 'etl', 'freshdesk', 'cd', 'bae'],
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
# # AIRBYTE SYNCS
# # =============================================================================


trigger_bsc_freshdesk_task = TrackedPythonOperator(
    task_id='trigger_bsc_freshdesk',
    python_callable=trigger_bsc_freshdesk,
    pipeline_name='bsc-freshdesk',
    client_id='bsc',
    data_type='bsc-freshdesk',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

airbyte_trigger_complete = DummyOperator(
    task_id='airbyte_trigger_complete',
    trigger_rule='all_done',
    dag=dag
)

# # =============================================================================
# # Customer Delight DASHBOARD  TASKS
# # =============================================================================

freshdesk_extract_extract_task = TrackedPythonOperator(
    task_id='freshdesk_extract_extract_task',
    python_callable=create_extract_wrapper(freshdesk_extract, 'bsc-freshdesk', 'bsc-freshdesk'),
    pipeline_name='bsc-freshdesk',
    client_id='bsc',
    data_type='bsc-freshdesk',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',  
    dag=dag
)

freshdesk_transform_ticket_task = TrackedPythonOperator(
    task_id='freshdesk_transform_ticket_task',
    python_callable=freshdesk_transform_ticket,
    pipeline_name='bsc-freshdesk',
    client_id='bsc',
    data_type='bsc-freshdesk',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

freshdesk_transform_conversation_task = TrackedPythonOperator(
    task_id='freshdesk_transform_conversation_task',
    python_callable=freshdesk_transform_conversation,
    pipeline_name='bsc-freshdesk',
    client_id='bsc',
    data_type='bsc-freshdesk',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

freshdesk_load_task = TrackedPythonOperator(
    task_id='freshdesk_load_task',
    python_callable=freshdesk_load,
    pipeline_name='bsc-freshdesk',
    client_id='bsc',
    data_type='bsc-freshdesk',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

# order pipeline completion marker
freshdesk_etl_complete = DummyOperator(
    task_id='freshdesk_etl_pipeline_complete',
    trigger_rule='all_done',
    dag=dag
)

create_view_freshdesk_agent_map = TrackedPythonOperator(
    task_id='create_view_freshdesk_agent_map',
    python_callable=create_view_freshdesk_agent_map,
    pipeline_name='freshdesk-views',
    client_id='bsc',
    data_type='freshdesk-data',
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

refresh_freshdesk_stored_procedures = TrackedPythonOperator(
    task_id='refresh_freshdesk_stored_procedures',
    python_callable=refresh_freshdesk_stored_procedures,
    pipeline_name='freshdesk-stored-procedures',
    client_id='bsc',
    data_type='freshdesk-data',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

refresh_freshdesk_dash = TrackedPythonOperator(
    task_id='refresh_freshdesk_dash',
    python_callable=refresh_freshdesk_dash,
    pipeline_name='freshdesk-dash',
    client_id='bsc',
    data_type='freshdesk-data',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)


# Initial setup with overlap prevention
start_task >> check_previous_run_task >> get_all_dates_task

# airbyte triggers

get_all_dates_task >> trigger_bsc_freshdesk_task >> airbyte_trigger_complete

# freshdesk pipeline tasks

airbyte_trigger_complete >> freshdesk_extract_extract_task >> freshdesk_transform_ticket_task >> freshdesk_transform_conversation_task >> freshdesk_load_task >> freshdesk_etl_complete 

# freshdesk dashboard tasks

freshdesk_etl_complete >> create_view_freshdesk_agent_map >> refresh_freshdesk_stored_procedures >> refresh_freshdesk_dash >> end_task   