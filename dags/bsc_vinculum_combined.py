# combined_vinculum_dag.py
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

sys.path.append('/opt/airflow')

# Import task functions for all pipelines
# Order Pull
from connections.bsc.vinculum.orderpull.extract import extract_data as order_extract
from connections.bsc.vinculum.orderpull.transform_v2 import transform_data as order_transform
from connections.bsc.vinculum.orderpull.load_v2 import load_data as order_load

# Invoice Detail
from connections.bsc.vinculum.invoicedetail.extract import extract_data as invoice_extract
from connections.bsc.vinculum.invoicedetail.transform_v2 import transform_data as invoice_transform
from connections.bsc.vinculum.invoicedetail.load_v2 import load_data as invoice_load

# Shipment Detail
from connections.bsc.vinculum.shipmentdetail.extract import extract_data as shipment_extract
from connections.bsc.vinculum.shipmentdetail.transform_v2 import transform_data as shipment_transform
from connections.bsc.vinculum.shipmentdetail.load import load_data as shipment_load 
# Return Detail
from connections.bsc.vinculum.returndetail.extract import extract_data as return_extract
from connections.bsc.vinculum.returndetail.transform import transform_data as return_transform
from connections.bsc.vinculum.returndetail.load import load_data as return_load
from utils.postgresconnector_v3 import PostgresConnector
# Inbound Detail
from connections.bsc.vinculum.inbounddetail.extract import extract_data as inbound_extract
from connections.bsc.vinculum.inbounddetail.transform import transform_data as inbound_transform_data
from connections.bsc.vinculum.inbounddetail.load import load_data as inbound_load_data
# STO Detail
from connections.bsc.vinculum.stodetail.extract import extract_data as sto_extract
from connections.bsc.vinculum.stodetail.transform import transform_data as sto_transform_data
from connections.bsc.vinculum.stodetail.load import load_data as sto_load_data

# Import utilities
from airflow import DAG
from plugins.operators.tracked_python_operator import TrackedPythonOperator

ALERT_EMAILS = [
    'ayushranjan@bombayshavingcompany.com',
    'ayushgoyal@bombayshavingcompany.com',
    'lakshay@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com'    
]


def upsert_vinculum_sales_report():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = f"""SELECT * FROM bsc.upsert_vinculum_sales_report(); """
    postgres.execute_query(query)

def upsert_vinculum_returns_report():

    postgres = PostgresConnector(db_prefix="warehouse_")
    query = "SELECT * FROM bsc.upsert_vinculum_returns_report(); "
    postgres.execute_query(query)

def upsert_vinculum_sto_report():

    postgres = PostgresConnector(db_prefix="warehouse_")
    query = "SELECT * FROM bsc.upsert_vinculum_sto_report(); "
    postgres.execute_query(query)

def refresh_mv_demand_supply():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = "REFRESH MATERIALIZED VIEW bsc.mv_demand_supply; "
    postgres.execute_query(query)


# def refresh_primary_sales():
#     postgres = PostgresConnector(db_prefix="warehouse_")
#     query = "REFRESH MATERIALIZED VIEW bsc.primary_sales; "
#     postgres.execute_query(query)


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
        START_DATE = datetime(2025, 6, 5, 0)
        END_DATE = datetime(2025, 6, 6, 0)
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
        ('orderpull', 'vinculum-orderpull'),
        ('invoicedetail', 'vinculum-invoicedetail'), 
        ('shipmentdetail', 'vinculum-shipmentdetail'),
        ('returndetail', 'vinculum-returndetail'),
        ('vinculum-inbounddetail', 'vinculum-inbounddetail'),
        ('vinculum-stodetail', 'vinculum-stodetail'),
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


# Handle parallel execution of materialized view refresh and create

def create_or_refresh_materialize_view(view_name, create_query):
    postgres = PostgresConnector(db_prefix="warehouse_")
    
    combined_query = f"""
        SET max_parallel_workers_per_gather = 0;
        SET max_parallel_workers = 0;

        
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_matviews 
                WHERE schemaname = 'bsc' 
                AND matviewname = '{view_name.lower()}'
            ) THEN
                REFRESH MATERIALIZED VIEW bsc.{view_name};
            ELSE
                {create_query}
            END IF;
        END $$;
        
        RESET max_parallel_workers_per_gather;
        RESET max_parallel_workers;
    """
    
    postgres.execute_query(combined_query)


# #=============================================================================
#  Primary Sales Materialzed View
# #=============================================================================

def create_refresh_primary_sales():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = f""" CREATE MATERIALIZED VIEW bsc.primary_sales AS
                WITH shopify_dedup AS (SELECT DISTINCT ON (sop.ordername) sop.ordername,
                                                                    sop.marketingtype
                                FROM bsc.shopify_operational_pnl_v2 sop
                                WHERE sop.ordername IS NOT NULL
                                ORDER BY sop.ordername, sop.updated_at DESC),
                base_sales AS (SELECT sales.fsn::text                                                 AS product_code,
                                    COALESCE(bpm.whsku, 'Unmapped'::character varying)::text        AS whsku,
                                    upper(COALESCE(bpm.whsku, 'Unmapped'::character varying)::text) AS whsku_key,
                                    CASE
                                        WHEN sales.event_type::text = 'Return'::text THEN - sales.item_quantity
                                        ELSE sales.item_quantity
                                        END                                                         AS quantity,
                                    NULL::text                                                      AS orderid,
                                    sales.order_id::text                                            AS externalordernumber,
                                    sales.buyer_invoice_date::date                                  AS invoicedate,
                                    sales.buyer_invoice_id::text                                    AS invoicenumber,
                                    sales.taxable_value::numeric                                    AS netsales,
                                    'Flipkart Internet Private Limited'::text                       AS party_name,
                                    'ECOM'::text                                                    AS channel,
                                    'Flipkart'::text                                                AS platform,
                                    CASE
                                        WHEN sales.event_type::text = 'Return'::text THEN 'RTO'::text
                                        WHEN sales.event_type::text = 'Sale'::text THEN 'Sale'::text
                                        ELSE NULL::text
                                        END                                                         AS saletype,
                                    'Flipkart Seller Panel'::text                                   AS datasource,
                                    CASE
                                        WHEN sales.event_type::text = 'Return'::text THEN sales.taxable_value::numeric
                                        ELSE 0::numeric
                                        END                                                         AS rto,
                                    'ECOM'::text                                                    AS subchannel,
                                    'FLIPKART'::text                                                AS demandchannel,
                                    'FLIPKART INTERNET PRIVATE LIMITED'::text                       AS financepartyname
                                FROM flipkart.marketplace_sales_tax_report sales
                                        LEFT JOIN (SELECT x.id,
                                                        x.channelid,
                                                        x.ipcode,
                                                        x.platformcode,
                                                        x.weight,
                                                        x.weightslabid,
                                                        x.anchorprice,
                                                        x.plannedprice,
                                                        x.businessmodel,
                                                        x.margintype,
                                                        x.marginon,
                                                        x.buyingmarginpercentage,
                                                        x.debitnotemarginpercentage,
                                                        x.unitmarginpercentage,
                                                        x.unitprice,
                                                        x.transferprice,
                                                        x.ishero,
                                                        x.isactive,
                                                        x.createdby,
                                                        x.createdat,
                                                        x.updatedby,
                                                        x.updatedat,
                                                        x.rn
                                                    FROM (SELECT pm_1.id,
                                                                pm_1.channelid,
                                                                pm_1.ipcode,
                                                                pm_1.platformcode,
                                                                pm_1.weight,
                                                                pm_1.weightslabid,
                                                                pm_1.anchorprice,
                                                                pm_1.plannedprice,
                                                                pm_1.businessmodel,
                                                                pm_1.margintype,
                                                                pm_1.marginon,
                                                                pm_1.buyingmarginpercentage,
                                                                pm_1.debitnotemarginpercentage,
                                                                pm_1.unitmarginpercentage,
                                                                pm_1.unitprice,
                                                                pm_1.transferprice,
                                                                pm_1.ishero,
                                                                pm_1.isactive,
                                                                pm_1.createdby,
                                                                pm_1.createdat,
                                                                pm_1.updatedby,
                                                                pm_1.updatedat,
                                                                row_number()
                                                                OVER (PARTITION BY pm_1.platformcode ORDER BY pm_1.createdat) AS rn
                                                        FROM flipkart.productmasterv2 pm_1
                                                        WHERE pm_1.isactive = true) x
                                                    WHERE x.rn = 1) pm ON pm.platformcode::text = sales.fsn::text
                                        LEFT JOIN bsc.productmaster bpm ON bpm.ipcode = pm.ipcode
                                UNION ALL
                                SELECT sales.asin::text                                                AS product_code,
                                    COALESCE(bpm.whsku, 'Unmapped'::character varying)::text        AS whsku,
                                    upper(COALESCE(bpm.whsku, 'Unmapped'::character varying)::text) AS whsku_key,
                                    sales.quantity::numeric                                         AS quantity,
                                    NULL::text                                                      AS orderid,
                                    sales.order_id::text                                            AS externalordernumber,
                                    sales.invoice_date                                              AS invoicedate,
                                    sales.invoice_number::text                                      AS invoicenumber,
                                    sales.tax_exclusive_gross::numeric                              AS netsales,
                                    'Amazon Seller Central'::text                                   AS party_name,
                                    'ECOM'::text                                                    AS channel,
                                    'Amazon'::text                                                  AS platform,
                                    CASE
                                        WHEN sales.transaction_type::text = 'Refund'::text THEN 'RTO'::text
                                        WHEN sales.transaction_type::text = 'Shipment'::text THEN 'Sale'::text
                                        ELSE NULL::text
                                        END                                                         AS saletype,
                                    'Amazon Seller Central Panel'::text                             AS datasource,
                                    CASE
                                        WHEN sales.transaction_type::text = 'Refund'::text THEN sales.tax_exclusive_gross::numeric
                                        ELSE 0::numeric
                                        END                                                         AS rto,
                                    'ECOM'::text                                                    AS subchannel,
                                    'AMAZON'::text                                                  AS demandchannel,
                                    'AMAZON FBA'::text                                              AS financepartyname
                                FROM amazon.fba_sales_tax_report sales
                                        LEFT JOIN amazon.productmaster pm ON pm.platformcode::text = sales.asin::text
                                        LEFT JOIN bsc.productmaster bpm ON bpm.ipcode = pm.ipcode
                                WHERE sales.transaction_type::text <> ALL (ARRAY ['FreeReplacement'::text, 'Cancel'::text])
                                UNION ALL
                                SELECT sales.style_id::text                                            AS product_code,
                                    COALESCE(bpm.whsku, 'Unmapped'::character varying)::text        AS whsku,
                                    upper(COALESCE(bpm.whsku, 'Unmapped'::character varying)::text) AS whsku_key,
                                    1::numeric                                                      AS quantity,
                                    NULL::text                                                      AS orderid,
                                    sales.seller_order_id::text                                     AS externalordernumber,
                                    sales.created_on::date                                          AS invoicedate,
                                    NULL::text                                                      AS invoicenumber,
                                    round(sales.final_amount / 1.18, 2)                             AS netsales,
                                    'Myntra Marketplace'::text                                      AS party_name,
                                    'ECOM'::text                                                    AS channel,
                                    'Myntra'::text                                                  AS platform,
                                    'Sale'::text                                                    AS saletype,
                                    'Myntra-Panel'::text                                            AS datasource,
                                    0::numeric                                                      AS rto,
                                    'ECOM OTHERS'::text                                             AS subchannel,
                                    'ECOM_OTHERS'::text                                             AS demandchannel,
                                    'MYNTRA SOR'::text                                              AS financepartyname
                                FROM myntra.marketplace_sales_tax_report sales
                                        LEFT JOIN myntra.productmaster pm ON pm.platformcode::text = sales.style_id::text AND
                                                                            (sales.business_model::text = 'SJIT'::text AND
                                                                            pm.channelid = 22 AND pm.isactive = true OR
                                                                            sales.business_model::text = 'PPMP'::text AND
                                                                            pm.channelid = 9 AND pm.isactive = true)
                                        LEFT JOIN bsc.productmaster bpm ON bpm.ipcode = pm.ipcode
                                WHERE sales.order_status::text <> ALL (ARRAY ['F'::text, 'RTO'::text])
                                UNION ALL
                                SELECT NULL::text                          AS product_code,
                                    vs.whsku,
                                    upper(vs.whsku::text)               AS whsku_key,
                                    vs.qty::numeric                     AS quantity,
                                    vs.orderid,
                                    vs.externalordernumber,
                                    vs.invoicedate,
                                    vs.invoicenumber,
                                    vs.netsale::numeric                 AS netsales,
                                    vs.partyname                        AS party_name,
                                    vs.channel,
                                    vs.platform,
                                    vs.saletype,
                                    'Vinculum Sales Report'::text       AS datasource,
                                    CASE
                                        WHEN vs.saletype::text = 'RTO'::text THEN vs.netsale::numeric
                                        ELSE 0::numeric
                                        END                             AS rto,
                                    vs.subchannel,
                                    vs.demandchannel,
                                    CASE
                                        WHEN vs.channel::text = 'D2C'::text AND sop.marketingtype::text = 'CORE'::text
                                            THEN 'D2C Core'::character varying
                                        WHEN vs.channel::text = 'D2C'::text THEN 'D2C Non-Core'::character varying
                                        ELSE vs.financepartyname
                                        END                             AS financepartyname
                                FROM bsc.vinculum_sales_report vs
                                        LEFT JOIN shopify_dedup sop ON sop.ordername::text = vs.externalordernumber::text
                                WHERE vs.orderlocation::text <> ALL
                                    (ARRAY ['M58'::text, 'M59'::text, 'M60'::text, 'M17'::text, 'M31'::text, 'M43'::text, 'M44'::text, 'M49'::text, 'M54'::text, 'M55'::text, 'M12'::text])
                                UNION ALL
                                SELECT NULL::text                          AS product_code,
                                    vr.whsku,
                                    upper(vr.whsku::text)               AS whsku_key,
                                    vr.qty * '-1'::integer::numeric     AS quantity,
                                    NULL::text                          AS orderid,
                                    vr.externalordernumber,
                                    vr.date                             AS invoicedate,
                                    vr.invoicenumber,
                                    vr.netsale * '-1'::integer::numeric AS netsales,
                                    vr.partyname                        AS party_name,
                                    vr.channel,
                                    vr.platform,
                                    vr.returntype                       AS saletype,
                                    'Vinculum Returns Report'::text     AS datasource,
                                    CASE
                                        WHEN vr.returntype::text = 'RTO'::text THEN vr.netsale * '-1'::integer::numeric
                                        ELSE 0::numeric
                                        END                             AS rto,
                                    vr.subchannel,
                                    vr.demandchannel,
                                    CASE
                                        WHEN vr.channel::text = 'D2C'::text AND sop.marketingtype::text = 'CORE'::text
                                            THEN 'D2C Core'::character varying
                                        WHEN vr.channel::text = 'D2C'::text THEN 'D2C Non-Core'::character varying
                                        ELSE vr.financepartyname
                                        END                             AS financepartyname
                                FROM bsc.vinculum_returns_report vr
                                        LEFT JOIN shopify_dedup sop ON sop.ordername::text = vr.externalordernumber::text
                                WHERE vr.returnlocation::text <> ALL
                                    (ARRAY ['M58'::text, 'M59'::text, 'M60'::text, 'M17'::text, 'M31'::text, 'M43'::text, 'M44'::text, 'M49'::text, 'M54'::text, 'M55'::text, 'M12'::text])
                                UNION ALL
                                SELECT NULL::text                 AS product_code,
                                    ts.whsku,
                                    upper(ts.whsku::text)      AS whsku_key,
                                    ts.quantity::numeric       AS quantity,
                                    NULL::text                 AS orderid,
                                    NULL::text                 AS externalordernumber,
                                    ts.date                    AS invoicedate,
                                    ts.voucher_number          AS invoicenumber,
                                    ts.amount::numeric         AS netsales,
                                    ts.party_name,
                                    ts.channel,
                                    ts.platform,
                                    ts.saletype,
                                    'Tally Sales Report'::text AS datasource,
                                    CASE
                                        WHEN ts.saletype::text = 'RTO'::text THEN ts.amount::numeric
                                        ELSE 0::numeric
                                        END                    AS rto,
                                    ''::text                   AS subchannel,
                                    ''::text                   AS demandchannel,
                                    ts.party_name              AS financepartyname
                                FROM bsc.tally_sales_report ts),
                date_matched_cogs AS (SELECT DISTINCT ON (sp.whsku_key, sp.invoicedate) sp.whsku_key,
                                                                                        sp.invoicedate,
                                                                                        s.mrp,
                                                                                        s.cogs
                                    FROM (SELECT DISTINCT base_sales.whsku_key,
                                                            base_sales.invoicedate
                                            FROM base_sales
                                            WHERE base_sales.whsku_key IS NOT NULL) sp
                                                JOIN bsc.mrpcogssnapshot s ON upper(s.whsku::text) = sp.whsku_key
                                    ORDER BY sp.whsku_key, sp.invoicedate, (s.valuationdate <= sp.invoicedate) DESC,
                                                (
                                                    CASE
                                                        WHEN s.valuationdate <= sp.invoicedate THEN s.valuationdate
                                                        ELSE NULL::date
                                                        END) DESC,
                                                (
                                                    CASE
                                                        WHEN s.valuationdate > sp.invoicedate THEN s.valuationdate
                                                        ELSE NULL::date
                                                        END), s.updatedat DESC)
            SELECT b.product_code,
                b.whsku,
                b.quantity,
                b.orderid,
                b.externalordernumber,
                b.invoicedate,
                b.invoicenumber,
                b.netsales,
                b.party_name,
                b.channel,
                b.platform,
                b.saletype,
                b.datasource,
                b.rto,
                dc.mrp,
                dc.cogs,
                b.subchannel,
                b.demandchannel,
                b.financepartyname
            FROM base_sales b
                    LEFT JOIN date_matched_cogs dc ON dc.whsku_key = b.whsku_key AND dc.invoicedate = b.invoicedate;
           """

    create_or_refresh_materialize_view("primary_sales", query)

# #=============================================================================
#  Refresh ECOM stock Analysis
# #=============================================================================

def refresh_ecom_stock_analysis():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = "CALL bsc.refresh_ecom_stock_analysis(); "
    postgres.execute_query(query)

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
    dag_id='combined_vinculum_etl',
    description='Combined ETL pipeline for all Vinculum data types',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    default_args=default_args,
    start_date=datetime(2025, 3, 9),
    catchup=False,
    max_active_runs=1,  # CRITICAL: Only allow 1 active run at a time
    max_active_tasks=10,  # Limit concurrent tasks within a DAG run
    tags=['etl', 'vinculum', 'combined', 'orders', 'invoice', 'shipment', 'returns'],
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
    trigger_rule='none_failed_min_one_success',  # Continue even if some tasks fail
    dag=dag
)

# =============================================================================
# ORDER PULL PIPELINE
# =============================================================================

# Order Pull tasks
order_extract_task = TrackedPythonOperator(
    task_id='order_extract_data',
    python_callable=create_extract_wrapper(order_extract, 'orderpull', 'order-details'),
    pipeline_name='vinculum-orderpull',
    client_id='bsc',
    data_type='order-details',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',  # Run if upstream didn't fail
    dag=dag
)

order_transform_task = TrackedPythonOperator(
    task_id='order_transform_data',
    python_callable=order_transform,
    pipeline_name='vinculum-orderpull',
    client_id='bsc',
    data_type='order-details',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

order_load_task = TrackedPythonOperator(
    task_id='order_load_data',
    python_callable=order_load,
    pipeline_name='vinculum-orderpull',
    client_id='bsc',
    data_type='order-details',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

# Order pipeline completion marker
order_complete = DummyOperator(
    task_id='order_pipeline_complete',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

# =============================================================================
# INVOICE DETAIL PIPELINE
# =============================================================================

invoice_extract_task = TrackedPythonOperator(
    task_id='invoice_extract_data',
    python_callable=create_extract_wrapper(invoice_extract, 'invoicedetail', 'vinculum-invoicedetail'),
    pipeline_name='vinculum-invoicedetail',
    client_id='bsc',
    data_type='order-details',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed_min_one_success',  # Run regardless of previous pipeline status
    dag=dag
)

invoice_transform_task = TrackedPythonOperator(
    task_id='invoice_transform_data',
    python_callable=invoice_transform,
    pipeline_name='vinculum-invoicedetail',
    client_id='bsc',
    data_type='order-details',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

invoice_load_task = TrackedPythonOperator(
    task_id='invoice_load_data',
    python_callable=invoice_load,
    pipeline_name='vinculum-invoicedetail',
    client_id='bsc',
    data_type='order-details',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

invoice_complete = DummyOperator(
    task_id='invoice_pipeline_complete',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

# =============================================================================
# SHIPMENT DETAIL PIPELINE
# =============================================================================

shipment_extract_task = TrackedPythonOperator(
    task_id='shipment_extract_data',
    python_callable=create_extract_wrapper(shipment_extract, 'shipmentdetail', 'vinculum-shipmentdetail'),
    pipeline_name='vinculum-shipmentdetail',
    client_id='bsc',
    data_type='shipment-details',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

shipment_transform_task = TrackedPythonOperator(
    task_id='shipment_transform_data',
    python_callable=shipment_transform,
    pipeline_name='vinculum-shipmentdetail',
    client_id='bsc',
    data_type='shipment-details',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

shipment_load_task = TrackedPythonOperator(
    task_id='shipment_load_data',
    python_callable=shipment_load,
    pipeline_name='vinculum-shipmentdetail',
    client_id='bsc',
    data_type='order-details',  # Note: keeping original data_type as in your code
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

shipment_complete = DummyOperator(
    task_id='shipment_pipeline_complete',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

# =============================================================================
# RETURN DETAIL PIPELINE
# =============================================================================

return_extract_task = TrackedPythonOperator(
    task_id='return_extract_data',
    python_callable=create_extract_wrapper(return_extract, 'returndetail', 'vinculum-returndetail'),
    pipeline_name='vinculum-returndetail',
    client_id='bsc',
    data_type='order-details',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

return_transform_task = TrackedPythonOperator(
    task_id='return_transform_data',
    python_callable=return_transform,
    pipeline_name='vinculum-returndetail',
    client_id='bsc',
    data_type='order-details',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

return_load_task = TrackedPythonOperator(
    task_id='return_load_data',
    python_callable=return_load,
    pipeline_name='vinculum-returndetail',
    client_id='bsc',
    data_type='order-details',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

return_complete = DummyOperator(
    task_id='return_pipeline_complete',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

upsert_vinculum_sales_report_task = PythonOperator(
    task_id="upsert_vinculum_sales_report_task",
    python_callable=upsert_vinculum_sales_report,
    trigger_rule='all_done',
)

upsert_vinculum_returns_report_task = PythonOperator(
    task_id="upsert_vinculum_returns_report_task",
    python_callable=upsert_vinculum_returns_report,
    trigger_rule='all_done',
)

refresh_mv_demand_supply_task = PythonOperator(
    task_id="refresh_mv_demand_supply_task",
    python_callable=refresh_mv_demand_supply,
    trigger_rule='all_done',
)

refresh_get_primary_sales_task = PythonOperator(
    task_id="refresh_get_primary_sales_task",
    python_callable=create_refresh_primary_sales,
    trigger_rule='all_done',
)

# =============================================================================
# INBOUND DETAIL PIPELINE
# =============================================================================

inbound_extract_task = TrackedPythonOperator(
    task_id='inbound_extract_data',
    python_callable=create_extract_wrapper(inbound_extract, 'vinculum-inbounddetail', 'vinculum-inbounddetail'),
    pipeline_name='vinculum-inbounddetail',
    client_id='bsc',
    data_type='vinculum-inbounddetail',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

inbound_transform_task = TrackedPythonOperator(
    task_id='inbound_transform_data',
    python_callable=inbound_transform_data,
    pipeline_name='vinculum-inbounddetail',
    client_id='bsc',
    data_type='vinculum-inbounddetail',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

inbound_load_task = TrackedPythonOperator(
    task_id='inbound_load_data',
    python_callable=inbound_load_data,
    pipeline_name='vinculum-inbounddetail',
    client_id='bsc',
    data_type='vinculum-inbounddetail',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

inbound_complete = DummyOperator(
    task_id='inbound_pipeline_complete',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

# =============================================================================
# STO DETAIL PIPELINE
# =============================================================================

sto_extract_task = TrackedPythonOperator(
    task_id='sto_extract_data',
    python_callable=create_extract_wrapper(sto_extract, 'vinculum-stodetail', 'vinculum-stodetail'),
    pipeline_name='vinculum-stodetail',
    client_id='bsc',
    data_type='vinculum-stodetail',
    is_first_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

sto_transform_task = TrackedPythonOperator(
    task_id='sto_transform_data',
    python_callable=sto_transform_data,
    pipeline_name='vinculum-stodetail',
    client_id='bsc',
    data_type='vinculum-stodetail',
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)

sto_load_task = TrackedPythonOperator(
    task_id='sto_load_data',
    python_callable=sto_load_data,
    pipeline_name='vinculum-stodetail',
    client_id='bsc',
    data_type='vinculum-stodetail',
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    trigger_rule='none_failed',
    dag=dag
)
    
sto_complete = DummyOperator(
    task_id='sto_pipeline_complete',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

upsert_vinculum_sto_report_task = PythonOperator(
    task_id="upsert_vinculum_sto_report_task",
    python_callable=upsert_vinculum_sto_report,
    trigger_rule='all_done',
)

# =============================================================================
# Task for Refresh ECOM stock Analysis
# =============================================================================

refresh_ecom_stock_analysis_task = PythonOperator(
    task_id="refresh_ecom_stock_analysis_task",
    python_callable=refresh_ecom_stock_analysis,
    trigger_rule='all_done',
)

# =============================================================================
# SET DEPENDENCIES - MODIFIED TO INCLUDE OVERLAP CHECK
# =============================================================================

# Initial setup with overlap prevention
start_task >> check_previous_run_task >> get_all_dates_task

# Order Pipeline (runs first)
get_all_dates_task >> order_extract_task >> order_transform_task >> order_load_task >> order_complete

# Invoice Pipeline (runs after order pipeline completes, regardless of success/failure)
order_complete >> invoice_extract_task >> invoice_transform_task >> invoice_load_task >> invoice_complete

# Shipment Pipeline (runs after invoice pipeline completes, regardless of success/failure)
invoice_complete >> shipment_extract_task >> shipment_transform_task >> shipment_load_task >> shipment_complete

# Return Pipeline (runs after shipment pipeline completes, regardless of success/failure)
shipment_complete >> return_extract_task >> return_transform_task >> return_load_task >> return_complete

# Inbound Pipeline (runs after return pipeline completes, regardless of success/failure)
return_complete >> inbound_extract_task >> inbound_transform_task >> inbound_load_task >> inbound_complete

# STO Pipeline (runs after inbound pipeline completes, regardless of success/failure)
inbound_complete >> sto_extract_task >> sto_transform_task >> sto_load_task >> sto_complete


# Final completion
sto_complete >> refresh_ecom_stock_analysis_task >> end_task

start >> sto_complete >> upsert_vinculum_sales_report_task >> upsert_vinculum_returns_report_task >> upsert_vinculum_sto_report_task >> refresh_mv_demand_supply_task >> refresh_get_primary_sales_task >> refresh_ecom_stock_analysis_task >> end_task