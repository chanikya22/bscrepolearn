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


# Import task google spent bae functions
from connections.bsc.shopify.google_ads.google_bae.extract import extract_data  as google_extract_data_bae
from connections.bsc.shopify.google_ads.google_bae.load import load_data as google_load_data_bae

# Import task google spent  bsc functions
from connections.bsc.shopify.google_ads.google_bsc.extract import extract_data as google_extract_data_bsc
from connections.bsc.shopify.google_ads.google_bsc.load import load_data as google_load_data_bsc

# Shopify BSC orders
from connections.bsc.shopify.orders.extract import extract_data as bsc_extract_shopify_orders
from connections.bsc.shopify.orders.transform_v2 import transform_data_to_postgres as bsc_transform_shopify_orders
from connections.bsc.shopify.orders.load import load_data as bsc_load_shopify_orders


# Shopify BAE orders
from connections.bsc.shopify.bomabe_orders.extract import extract_data as bae_extract_shopify_orders
from connections.bsc.shopify.bomabe_orders.transform import transform_data_to_postgres as bae_transform_shopify_orders
from connections.bsc.shopify.bomabe_orders.load import load_data as bae_load_shopify_orders

# Load Gold Table/Dashboards functions 
from connections.bsc.shopify.shopify_pnl.load import load_v2 as load_v2

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
        ('bsc-shopify-orders', 'bsc-shopify-orders'),
        ('bae-shopify-orders', 'bae-shopify-orders'),
        ('google-bae-ads', 'google-bae-ads'),
        ('google-bsc-ads', 'google-bsc-ads')
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

def trigger_bsc_meta_spent(**context):
    connection_id = "8ef73830-c29a-49a6-b915-db01ca2238b9"  
    return run_airbyte_connection(connection_id)

def trigger_bae_meta_spent(**context):
    connection_id = "8c060668-8789-4d2e-899c-3fd614690f61"  
    return run_airbyte_connection(connection_id)

def trigger_bae_GA4(**context):
    connection_id = "82399968-7add-4116-a973-afd5263574da"  
    return run_airbyte_connection(connection_id)


def trigger_bsc_GA4(**context):
    connection_id = "3b9b0a64-b0ab-4567-98f5-b6aa2dfd8c7c"  
    return run_airbyte_connection(connection_id)


# # =============================================================================
# DASHBOARD REFRESH FUNCTIONS
# # =============================================================================

def refresh_operational_pnl_order_details_view():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = f"""SELECT * FROM bsc.refresh_shopify_order_details(); """
    postgres.execute_query(query)

def refresh_shopify_affiliate_validation_v2():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """BEGIN;
SET max_parallel_workers_per_gather = 0;
REFRESH MATERIALIZED VIEW bsc.shopify_affiliate_validation_v2;
RESET max_parallel_workers_per_gather;
COMMIT;"""
    postgres.execute_query(query)

def refresh_shopify_marketplace_summary_v2():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """BEGIN;
SET max_parallel_workers_per_gather = 0;
REFRESH MATERIALIZED VIEW bsc.shopify_marketplace_summary_v2;
RESET max_parallel_workers_per_gather;
COMMIT;"""
    postgres.execute_query(query)

def create_view_shopify_pnl_combined_v2():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = f"""CREATE MATERIALIZED VIEW bsc.shopify_pnl_combined_v2 AS
                        WITH normalized_utm_mapping AS (
                            SELECT DISTINCT ON (
                                UPPER("GA_SOURCEMEDIUM")
                                )
                                UPPER("GA_SOURCEMEDIUM") AS utm_source_medium,
                                UPPER("Final_Channel") AS channel,
                                UPPER("Type") AS type,
                                UPPER("Final_Source") AS source
                            FROM shopify.ga_channel_mapping
                            ORDER BY
                                UPPER("GA_SOURCEMEDIUM")
                        ),

                            combined_ga_data AS (
                                -- BSC Google Analytics data
                                SELECT
                                    UPPER(ga."sessionSource") || ' / ' || UPPER(ga."sessionMedium") AS utm_source_medium,
                                    ga."sessions",
                                    ga."totalUsers",
                                    ga.date::date AS date,
                                    'Bombay Shaving Company' AS store
                                FROM bsc.googleanalytics_traffic_sources ga

                                UNION ALL

                                -- Bombae Google Analytics data
                                SELECT
                                    UPPER(ga."sessionSource") || ' / ' || UPPER(ga."sessionMedium") AS utm_source_medium,
                                    ga."sessions",
                                    ga."totalUsers",
                                    ga.date::date AS date,
                                    'BOMBAE' AS store
                                FROM bsc.googleanalytics_bae_traffic_sources ga
                            ),

                            ga_data AS (
                                SELECT
                                    ga.date,
                                    ga.store,
                                    CASE
                                        WHEN nutm.type IS NOT NULL THEN nutm.type
                                        ELSE 'CORE'
                                        END as marketingtype,
                                    CASE
                                        WHEN nutm.channel IS NOT NULL THEN nutm.channel
                                        ELSE 'UNPAID'
                                        END as marketingchannel,
                                    CASE
                                        WHEN nutm.source IS NOT NULL THEN nutm.source
                                        ELSE 'DIRECT'
                                        END as marketingsource,
                                    SUM(ga."sessions") as total_sessions
                                FROM combined_ga_data ga
                                        LEFT JOIN normalized_utm_mapping nutm
                                                    ON ga.utm_source_medium = nutm.utm_source_medium
                                GROUP BY
                                    ga.date,
                                    ga.store,
                                    CASE
                                        WHEN nutm.type IS NOT NULL THEN nutm.type
                                        ELSE 'CORE'
                                        END,
                                    CASE
                                        WHEN nutm.channel IS NOT NULL THEN nutm.channel
                                        ELSE 'UNPAID'
                                        END,
                                    CASE
                                        WHEN nutm.source IS NOT NULL THEN nutm.source
                                        ELSE 'DIRECT'
                                        END
                            )

                        SELECT

                            s.orderdate :: date,
                            s.marketingtype,
                            s.marketingsource,
                            s.marketingchannel,
                            s.store AS store,
                            MAX(s.brandname) as brandname,
                            SUM(COALESCE(s.mrpsales,0)) AS mrpsales,
                            SUM(COALESCE(s.grosssales, 0)) AS total_gross,
                            SUM(COALESCE(s.mrpdiscount,0)) AS total_discount,
                            ROUND(SUM(COALESCE(s.grosssales, 0)) - SUM(COALESCE(s.cancelledsales, 0))) AS totalsales_ex_canc,
                            COUNT(DISTINCT CASE
                                            WHEN s.quantity <>0 AND UPPER(s.orderstatus) <>'SHIPPED & RETURNED' OR s.orderstatus is null
                                                THEN s.ordername  END ) AS total_order,
                            SUM(COALESCE(s.quantity, 0))::numeric AS total_quantity,
                            ROUND(SUM(COALESCE(s.allocated_marketing_spend, 0)), 2) AS allocated_marketing_spend,
                            ROUND(SUM(COALESCE(s.affiliatemarketingspend, 0)), 2) AS affiliated_marketing_spend,
                            COUNT(DISTINCT CASE WHEN s.new_customer = 'TRUE' THEN s.phone END) AS new_customer,
                            COUNT(DISTINCT s.phone) 											AS total_customer,
                            COUNT(DISTINCT CASE
                                            WHEN UPPER(s.orderstatus)='CANCELLED'
                                                THEN s.ordername END ) AS canc_orders,
                            ROUND(SUM(COALESCE(s.shipping_price,0)),2) AS shipping_price,
                            ROUND(SUM(COALESCE(s.rtosales,0)),2) AS rtosales,
                            ROUND(SUM(COALESCE(s.tax,0)),2) AS TAX,
                            ROUND(SUM(COALESCE(s.netsales,0)),2) AS netsales,
                            ROUND(SUM(COALESCE(s.cogs,0)),2) AS cogs,
                            ROUND(SUM(COALESCE(s.logisticscost,0)),2) AS logistics,
                            ROUND(SUM(COALESCE(s.cm1,0)),2) AS cm1,
                            ROUND(SUM(COALESCE(s.cm2,0)),2) AS cm2,

                            -- GA sessions from joined table
                            COALESCE(g.total_sessions, 0) AS total_sessions

                        FROM bsc.shopify_operational_pnl_v2 s
                                LEFT JOIN ga_data g ON s.store = g.store
                            AND s.orderdate :: date = g.date
                            AND s.marketingtype = g.marketingtype
                            AND s.marketingchannel = g.marketingchannel
                            AND s.marketingsource = g.marketingsource
                        GROUP BY
                            s.store,
                            s.orderdate :: date,
                            s.marketingtype,
                            s.marketingsource,
                            s.marketingchannel,
                            total_sessions
                        ORDER BY s.orderdate :: date DESC, total_gross DESC;"""
    create_or_refresh_view('shopify_pnl_combined_v2', query)


def create_view_shopify_sales_analysisV2_v2():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = f"""
    
    CREATE MATERIALIZED VIEW bsc.shopify_sales_analysisV2_v2 AS
                    WITH normalized_utm_mapping AS (
                        SELECT DISTINCT ON (
                            UPPER("GA_SOURCEMEDIUM")
                            )
                            UPPER("GA_SOURCEMEDIUM") AS utm_source_medium,
                            UPPER("Final_Channel") AS channel,
                            UPPER("Type") AS type,
                            UPPER("Final_Source") AS source
                        FROM shopify.ga_channel_mapping
                        ORDER BY
                            UPPER("GA_SOURCEMEDIUM")
                    ),

                        combined_ga_data AS (
                            -- BSC Google Analytics data
                            SELECT
                                UPPER(ga."sessionSource") || ' / ' || UPPER(ga."sessionMedium") AS utm_source_medium,
                                ga."engagedSessions",
                                ga."totalUsers",
                                ga.date::date AS date,
                                'Bombay Shaving Company' AS store
                            FROM bsc.googleanalytics_traffic_acquisition_session_source_medium_repor ga

                            UNION ALL

                            -- Bombae Google Analytics data
                            SELECT
                                UPPER(ga."sessionSource") || ' / ' || UPPER(ga."sessionMedium") AS utm_source_medium,
                                ga."engagedSessions",
                                ga."totalUsers",
                                ga.date::date AS date,
                                'BOMBAE' AS store
                            FROM bsc.googleanalytics_bae_traffic_acquisition_session_source_medium_r ga
                        ),

                        ga_data AS (
                            SELECT
                                ga.date,
                                ga.store,
                                CASE
                                    WHEN nutm.type IS NOT NULL THEN nutm.type
                                    ELSE 'CORE'
                                    END as marketingtype,
                                CASE
                                    WHEN nutm.channel IS NOT NULL THEN nutm.channel
                                    ELSE 'UNPAID'
                                    END as marketingchannel,
                                CASE
                                    WHEN nutm.source IS NOT NULL THEN nutm.source
                                    ELSE 'DIRECT'
                                    END as marketingsource,
                                SUM(ga."engagedSessions") as total_sessions
                            FROM combined_ga_data ga
                                    LEFT JOIN normalized_utm_mapping nutm
                                                ON ga.utm_source_medium = nutm.utm_source_medium
                            GROUP BY
                                ga.date,
                                ga.store,
                                CASE
                                    WHEN nutm.type IS NOT NULL THEN nutm.type
                                    ELSE 'CORE'
                                    END,
                                CASE
                                    WHEN nutm.channel IS NOT NULL THEN nutm.channel
                                    ELSE 'UNPAID'
                                    END,
                                CASE
                                    WHEN nutm.source IS NOT NULL THEN nutm.source
                                    ELSE 'DIRECT'
                                    END
                        )

                    SELECT

                        s.orderdate :: date,
                        s.marketingtype,
                        s.marketingsource,
                        s.marketingchannel,
                        s.store AS store,
                        MAX(s.brandname) as brandname,
                        SUM(COALESCE(s.grosssales, 0)) AS total_gross,
                        SUM(COALESCE(s.mrpdiscount,0)) AS total_discount,
                        ROUND(SUM(COALESCE(s.grosssales, 0)) - SUM(COALESCE(s.cancelledsales, 0))) AS totalsales_ex_canc,
                        COUNT(DISTINCT CASE
                                        WHEN s.quantity <>0 AND UPPER(s.orderstatus) <>'SHIPPED & RETURNED' OR s.orderstatus is null
                                            THEN s.ordername  END ) AS total_order,
                        SUM(COALESCE(s.quantity, 0))::numeric AS total_quantity,
                        ROUND(SUM(COALESCE(s.allocated_marketing_spend, 0)), 2) AS allocated_marketing_spend,
                        ROUND(SUM(COALESCE(s.affiliatemarketingspend, 0)), 2) AS affiliated_marketing_spend,
                        COUNT(DISTINCT CASE WHEN s.new_customer = 'TRUE' THEN s.phone END) AS new_customer,
                        COUNT(DISTINCT s.phone) 											AS total_customer,
                        COUNT(DISTINCT CASE
                                        WHEN UPPER(s.orderstatus)='CANCELLED'
                                            THEN s.ordername END ) AS canc_orders,
                        SUM(COALESCE(CASE
                                        WHEN UPPER(s.orderstatus) = 'CANCELLED' THEN s.quantity ELSE 0 END ,0)) AS canc_quantity  ,

                        -- GA sessions from joined table
                        COALESCE(g.total_sessions, 0) AS total_sessions

                    FROM bsc.shopify_operational_pnl_v2 s
                            LEFT JOIN ga_data g ON s.store = g.store
                        AND s.orderdate :: date = g.date
                        AND s.marketingtype = g.marketingtype
                        AND s.marketingchannel = g.marketingchannel
                        AND s.marketingsource = g.marketingsource
                    GROUP BY
                        s.store,
                        s.orderdate :: date,
                        s.marketingtype,
                        s.marketingsource,
                        s.marketingchannel,
                        total_sessions
                    ORDER BY s.orderdate :: date DESC, total_gross DESC;
                    
                    """
    create_or_refresh_view('shopify_sales_analysisV2_v2', query)


def refresh_ads_performance_summary_v2():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """BEGIN;
SET max_parallel_workers_per_gather = 0;
REFRESH MATERIALIZED VIEW bsc.meta_performancev2;
REFRESH MATERIALIZED VIEW bsc.google_performance;
REFRESH MATERIALIZED VIEW bsc.gm_data;
RESET max_parallel_workers_per_gather;
COMMIT;"""
    postgres.execute_query(query)

def create_or_refresh_view(view_name, create_query):
    postgres = PostgresConnector(db_prefix="warehouse_")

    # Check if view exists (PostgreSQL stores names in lowercase)
    check_query = f"""
        SELECT EXISTS (
            SELECT 1 FROM pg_matviews
            WHERE schemaname = 'bsc'
            AND matviewname = '{view_name.lower()}'
        );
    """

    # Use read_query which definitely exists in your class
    result = postgres.read_query(check_query)
    exists = result.iloc[0, 0] if not result.empty else False

    if exists:
        # Refresh if exists
        postgres.execute_query(f"""BEGIN;
SET max_parallel_workers_per_gather = 0;
REFRESH MATERIALIZED VIEW bsc.{view_name};
RESET max_parallel_workers_per_gather;
COMMIT;""")
    else:
        # Create if doesn't exist
        postgres.execute_query(f"""BEGIN;
SET max_parallel_workers_per_gather = 0;
{create_query}
RESET max_parallel_workers_per_gather;
COMMIT;""")


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
    dag_id='bsc_d2c_combined_pipeline',
    description='End to end  pipeline for all Shopify orders marketing spent and dashboard refresh for BSC',
    schedule_interval='30 23,11 * * *',  # Daily at 5:00 AM AND 5:00 PM IST
    default_args=default_args,
    start_date=datetime(2025, 3, 9),
    catchup=False,
    max_active_runs=1,  # CRITICAL: Only allow 1 active run at a time
    max_active_tasks=10,  # Limit concurrent tasks within a DAG run
    tags=['shopify', 'operational', 'bsc', 'etl', 'cred', 'pop', 'bae'],
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

trigger_bsc_meta_spent_task = TrackedPythonOperator(
    task_id='trigger_bsc_meta_spent',
    python_callable=trigger_bsc_meta_spent,
    pipeline_name='bsc-meta-spent',
    client_id='bsc',
    data_type='bsc-meta-spent',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

trigger_bae_meta_spent_task = TrackedPythonOperator(
    task_id='trigger_bae_meta_spent',
    python_callable=trigger_bae_meta_spent,
    pipeline_name='bae-meta-spent',
    client_id='bsc',
    data_type='bae-meta-spent',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

trigger_bae_GA4_task = TrackedPythonOperator(
    task_id='trigger_bae_GA4',
    python_callable=trigger_bae_GA4,
    pipeline_name='bae-GA4',
    client_id='bsc',
    data_type='bae-GA4',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

trigger_bsc_GA4_task = TrackedPythonOperator(
    task_id='trigger_bsc_GA4',
    python_callable=trigger_bsc_GA4,
    pipeline_name='bsc-GA4',
    client_id='bsc',
    data_type='bsc-GA4',
    is_last_task=False,
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
# # EXTRACT BAE GOOGLE ADS DATA
# # =============================================================================


# Define the extract task
google_bae_extract_task = TrackedPythonOperator(
    task_id='bae_extract_google_ads_data',
    python_callable=create_extract_wrapper(google_extract_data_bae, 'google-bae-ads', 'google-bae-ads'),
    op_kwargs={
        'report_types': ['campaigns', 'keywords', 'ads']  # Extract all report types else mention specific ones
    },
    pipeline_name='google-bae-ads',
    client_id='bsc',
    data_type='google-bae-ads',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

# Load task - loads the extracted data to database
google_bae_load_task = TrackedPythonOperator(
    task_id='bae_load_google_ads_data',
    python_callable=google_load_data_bae,
    pipeline_name='google-bae-ads',
    client_id='bsc',
    data_type='google-bae-ads',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

# bae google ads completion marker
bae_google_ads_complete = DummyOperator(
    task_id='bae_google_ads_pipeline_complete',
    trigger_rule='all_done',
    dag=dag
)



# # =============================================================================
# # EXTRACT BSC GOOGLE ADS DATA
# # =============================================================================


# Define the extract task
google_bsc_extract_task = TrackedPythonOperator(
    task_id='bsc_extract_google_ads_data',
    python_callable=create_extract_wrapper(google_extract_data_bsc, 'google-bsc-ads', 'google-bsc-ads'),
    op_kwargs={
        'report_types': ['campaigns', 'keywords', 'ads']  # Extract all report types else mention specific ones
    },
    pipeline_name='google-bsc-ads',
    client_id='bsc',
    data_type='google-bsc-ads',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

# Load task - loads the extracted data to database
google_bsc_load_task = TrackedPythonOperator(
    task_id='bsc_load_google_ads_data',
    python_callable=google_load_data_bsc,
    pipeline_name='google-bsc-ads',
    client_id='bsc',
    data_type='google-bsc-ads',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

# bsc google ads completion marker
bsc_google_ads_complete = DummyOperator(
    task_id='bsc_google_ads_pipeline_complete',
    trigger_rule='all_done',
    dag=dag
)


# # =============================================================================
# # LOAD BSC SHOPIFY ORDERS PIPELINE
# # =============================================================================

# extract order tasks
bsc_order_extract_task = TrackedPythonOperator(
    task_id='bsc_order_extract_data',
    python_callable=create_extract_wrapper(bsc_extract_shopify_orders, 'bsc-shopify-orders', 'bsc-shopify-orders'),
    pipeline_name='bsc-shopify-orders',
    client_id='bsc',
    data_type='bsc-shopify-orders',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',  
    dag=dag
)

bsc_order_transform_task = TrackedPythonOperator(
    task_id='bsc_order_transform_data',
    python_callable=bsc_transform_shopify_orders,
    pipeline_name='bsc-shopify-orders',
    client_id='bsc',
    data_type='bsc-shopify-orders',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)


bsc_order_load_task = TrackedPythonOperator(
    task_id='bsc_order_load_data',
    python_callable=bsc_load_shopify_orders,
    pipeline_name='bsc-shopify-orders',
    client_id='bsc',
    data_type='bsc-shopify-orders',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)


# order pipeline completion marker
bsc_order_complete = DummyOperator(
    task_id='bsc_order_pipeline_complete',
    trigger_rule='all_done',
    dag=dag
)


# # =============================================================================
# # LOAD BAE SHOPIFY ORDERS PIPELINE
# # =============================================================================



# extract order tasks
bae_order_extract_task = TrackedPythonOperator(
    task_id='bae_order_extract_data',
    python_callable=create_extract_wrapper(bae_extract_shopify_orders, 'bae-shopify-orders', 'bae-shopify-orders'),
    pipeline_name='bae-shopify-orders',
    client_id='bsc',
    data_type='bae-shopify-orders',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',  
    dag=dag
)

# transform orders
bae_order_transform_task = TrackedPythonOperator(
    task_id='bae_order_transform_data',
    python_callable=bae_transform_shopify_orders,
    pipeline_name='bae-shopify-orders',
    client_id='bsc',
    data_type='bae-shopify-orders',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

# load orders
bae_order_load_task = TrackedPythonOperator(
    task_id='bae_order_load_data',
    python_callable=bae_load_shopify_orders,
    pipeline_name='bae-shopify-orders',
    client_id='bsc',
    data_type='bae-shopify-orders',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)


# order pipeline completion marker
bae_order_complete = DummyOperator(
    task_id='bae_order_pipeline_complete',
    trigger_rule='all_done',
    dag=dag
)


# # =============================================================================
# # LOAD SHOPIFY OPERATIONAL PNL ORDERS PIPELINE
# # =============================================================================


shopify_load_operational_pnl = TrackedPythonOperator(
    task_id='shopify_load_operational_pnl',
    python_callable=load_v2,
    pipeline_name='shopify-operational-pnl',
    client_id='bsc',
    data_type='shopify-operational-pnl',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done',
    dag=dag
)

# # =============================================================================
# # DASHBOARD REFRESH TASKS
# # =============================================================================

refresh_operational_pnl_order_details_view = TrackedPythonOperator(
    task_id='refresh_operational_pnl_order_details_view',
    python_callable=refresh_operational_pnl_order_details_view,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)


refresh_shopify_affiliate_validation_v2 = TrackedPythonOperator(
    task_id='refresh_shopify_affiliate_validation_v2',
    python_callable=refresh_shopify_affiliate_validation_v2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

refresh_shopify_marketplace_summary_v2 = TrackedPythonOperator(
    task_id='refresh_shopify_marketplace_summary_v2',
    python_callable=refresh_shopify_marketplace_summary_v2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

create_view_shopify_pnl_combined_v2 = TrackedPythonOperator(
    task_id='create_view_shopify_pnl_combined_v2',
    python_callable=create_view_shopify_pnl_combined_v2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

create_view_shopify_sales_analysisV2_v2 = TrackedPythonOperator(
    task_id='create_view_shopify_sales_analysisV2_v2',
    python_callable=create_view_shopify_sales_analysisV2_v2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

refresh_view_shopify_ads_performance_summary_v2 = TrackedPythonOperator(
    task_id='refresh_view_shopify_ads_performance_summary_v2',
    python_callable=refresh_ads_performance_summary_v2,
    pipeline_name='shopify-dashboard-views',
    client_id='bsc',
    data_type='pnl-data',
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='all_done', 
    dag=dag
)

# Initial setup with overlap prevention
start_task >> check_previous_run_task >> get_all_dates_task

# airbyte triggers

get_all_dates_task >> trigger_bsc_meta_spent_task >> trigger_bae_meta_spent_task >> trigger_bae_GA4_task >> trigger_bsc_GA4_task >> airbyte_trigger_complete

# google ads pipelines

airbyte_trigger_complete >> google_bae_extract_task >> google_bae_load_task >> bae_google_ads_complete

bae_google_ads_complete >> google_bsc_extract_task >> google_bsc_load_task >> bsc_google_ads_complete


# BAE Order Pipeline (runs first)
bsc_google_ads_complete >> bae_order_extract_task >> bae_order_transform_task >> bae_order_load_task >> bae_order_complete

# BSC Order Pipeline (runs second)
bae_order_complete >> bsc_order_extract_task >> bsc_order_transform_task >> bsc_order_load_task >> bsc_order_complete

bsc_order_complete >> shopify_load_operational_pnl 

shopify_load_operational_pnl >> refresh_operational_pnl_order_details_view >> refresh_shopify_affiliate_validation_v2 >> refresh_shopify_marketplace_summary_v2 >> create_view_shopify_pnl_combined_v2 >> create_view_shopify_sales_analysisV2_v2 >> refresh_view_shopify_ads_performance_summary_v2 >> end_task


