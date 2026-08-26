"""
Amazon Data Pipeline DAG (V2 - Dynamic Date Processing)
--------------------------------------------------------
This DAG orchestrates the Amazon data pipeline process including:
- Dynamic date detection from database
- Iterative processing for missing dates (from last processed + 1 to T-2)
- KK/RK sales data scraping and processing
- Ad reports processing (non-blocking - failures only send alerts)
- DSR report generation

Key Changes:
- Checks database for last processed date before running
- Processes all missing dates up to T-2
- Ad health check failures don't stop DSR pipeline
"""

import logging
from datetime import datetime, timedelta, date
from time import sleep
from typing import Optional
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from utils.postgresconnector_v3 import PostgresConnector
from airflow.utils.trigger_rule import TriggerRule
from pendulum import timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


from connections.bsc.amazon.sales.extract import amazon_kk_rk_scraper
from connections.bsc.amazon.sales.load import upload_KK_RK_files
from connections.bsc.amazon.ads.extract_load import call_amazon_ad_reports_api
from connections.bsc.dsr.process_generate import process_dsr, generate_dsr, refresh_business_overview, process_central_dsr_dump

# Initialize database connection
postgres = PostgresConnector(db_prefix="warehouse_")

# Define the timezone
local_tz = timezone('Asia/Kolkata')

# =============================================================================
# DYNAMIC DATE FUNCTIONS
# =============================================================================

def get_last_processed_date_from_db(table: str, column: str, channelid: int = 0) -> Optional[date]:
    """
    Get the last processed date from the database.
    
    Args:
        table: Database table name
        column: Date column name
        channelid: Channel ID filter (0 for no filter)
    
    Returns:
        The last processed date or None if no data exists
    """
    logger.info(f"Fetching last processed date from {table}.{column} for channelid={channelid}")
    
    try:
        if channelid == 0:
            query = f"SELECT MAX({column}) FROM {table};"
        else:
            query = f"""
                SELECT MAX(s.{column}) 
                FROM {table} s 
                LEFT JOIN Amazon.ProductMaster pm ON pm.id = s.PlatformProductId 
                WHERE pm.ChannelId = {channelid};
            """
        
        logger.info(f"Executing query: {query}")
        result = postgres.read_query(query, as_dict=True)
        
        if result and len(result) > 0 and result[0]['max'] is not None:
            last_date = result[0]['max']
            if isinstance(last_date, datetime):
                last_date = last_date.date()
            logger.info(f"Last processed date in {table}: {last_date}")
            return last_date
        else:
            logger.warning(f"No data found in {table}.{column}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching last processed date: {str(e)}")
        raise

def calculate_date_range(**context) -> dict:
    """
    Calculate the date range to process based on database state.
    
    Returns dict with:
        - start_date: First date to process (last_processed + 1)
        - end_date: Last date to process (T-2 or max available)
        - dates_to_process: List of dates to process
        - has_dates_to_process: Boolean flag
    """
    logger.info("Calculating date range to process...")
    
    # Get the maximum available date (T-2)
    max_available_date = (datetime.now() - timedelta(days=2)).date()
    logger.info(f"Maximum available date (T-2): {max_available_date}")
    
    # Get last processed date from Sales table for both RK and KK channels
    rk_last_processed_date = get_last_processed_date_from_db('Amazon.Sales', 'valuationdate', 1)
    kk_last_processed_date = get_last_processed_date_from_db('Amazon.Sales', 'valuationdate', 2)
    
    logger.info(f"RK last processed date: {rk_last_processed_date}")
    logger.info(f"KK last processed date: {kk_last_processed_date}")
    
    # Use the most recent date from either channel
    if rk_last_processed_date is None and kk_last_processed_date is None:
        # If no data exists for either channel, start from beginning of current month - 2 days
        last_processed_date = None
        start_date = (datetime.now() - timedelta(days=2)).replace(day=1).date()
        logger.info(f"No previous data found for either channel. Starting from: {start_date}")
    else:
        # Find the most recent date from both channels
        dates_to_compare = []
        if rk_last_processed_date is not None:
            dates_to_compare.append(rk_last_processed_date)
        if kk_last_processed_date is not None:
            dates_to_compare.append(kk_last_processed_date)
        
        last_processed_date = max(dates_to_compare)
        start_date = last_processed_date + timedelta(days=1)
        logger.info(f"Most recent processed date: {last_processed_date}. Starting from: {start_date}")
    
    # Calculate dates to process
    dates_to_process = []
    current_date = start_date
    
    while current_date <= max_available_date:
        dates_to_process.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    result = {
        'start_date': start_date.strftime('%Y-%m-%d') if dates_to_process else None,
        'end_date': max_available_date.strftime('%Y-%m-%d') if dates_to_process else None,
        'dates_to_process': dates_to_process,
        'has_dates_to_process': len(dates_to_process) > 0,
        'num_dates': len(dates_to_process),
        'rk_last_processed': rk_last_processed_date.strftime('%Y-%m-%d') if rk_last_processed_date else None,
        'kk_last_processed': kk_last_processed_date.strftime('%Y-%m-%d') if kk_last_processed_date else None,
        'last_processed_date': last_processed_date.strftime('%Y-%m-%d') if last_processed_date else None
    }
    
    logger.info(f"Date calculation result: {result}")
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='date_range', value=result)
    
    return result

def check_if_dates_to_process(**context) -> str:
    """
    Branch operator to check if there are dates to process.
    Returns task_id to execute next.
    """
    date_range = context['ti'].xcom_pull(key='date_range', task_ids='calculate_date_range')
    
    if date_range and date_range.get('has_dates_to_process', False):
        logger.info(f"Found {date_range['num_dates']} dates to process")
        return 'process_sales_data'
    else:
        logger.info("No dates to process. Data is up to date.")
        return 'skip_to_end'

# =============================================================================
# SALES DATA PROCESSING FUNCTIONS
# =============================================================================

def process_sales_data(**context):
    """
    Process sales data for all dates in the range.
    Iterates through each date and processes it.
    """
    date_range = context['ti'].xcom_pull(key='date_range', task_ids='calculate_date_range')
    
    if not date_range or not date_range.get('has_dates_to_process'):
        logger.info("No dates to process")
        return {'status': 'skipped', 'reason': 'No dates to process'}
    
    dates_to_process = date_range['dates_to_process']
    logger.info(f"Processing {len(dates_to_process)} dates: {dates_to_process}")
    
    results = {
        'processed_dates': [],
        'extract_results': [],
        'load_results': [],
        'failed_dates': []
    }
    
    for process_date in dates_to_process:
        logger.info(f"{'='*50}")
        logger.info(f"Processing date: {process_date}")
        logger.info(f"{'='*50}")
        
        try:
            # Step 1: Extract sales data for this date
            logger.info(f"Extracting sales data for {process_date}")
            if amazon_kk_rk_scraper:
                extract_result = amazon_kk_rk_scraper(target_date=process_date)
                logger.info(f"Extract result for {process_date}: {extract_result}")
                results['extract_results'].append({'date': process_date, 'result': extract_result})
            else:
                logger.warning("Sales extract function not available")
                extract_result = "Extract function not available"
            
            # Step 2: Load sales data for this date
            logger.info(f"Loading sales data for {process_date}")
            if upload_KK_RK_files:
                load_result = upload_KK_RK_files(process_date)
                logger.info(f"Load result for {process_date}: {load_result}")
                results['load_results'].append({'date': process_date, 'result': load_result})
            else:
                logger.warning("Sales load function not available")
                load_result = "Load function not available"
            
            results['processed_dates'].append(process_date)
            logger.info(f"Successfully processed date: {process_date}")
        except Exception as e:
            logger.error(f"Error processing date: {process_date}: {str(e)}")
            results['failed_dates'].append(process_date)
    
    # Push results to XCom
    context['ti'].xcom_push(key='sales_processing_results', value=results)
    
    logger.info(f"All sales data processing complete. Processed: {len(results['processed_dates'])} dates, Failed: {len(results['failed_dates'])}")
    
    return results


# =============================================================================
# AD REPORT FUNCTIONS (Non-blocking)
# =============================================================================

def process_ad_reports(**context):
    """
    Process all ad reports for the date range.
    Failures are logged and emailed but don't stop execution.
    """
    date_range = context['ti'].xcom_pull(key='date_range', task_ids='calculate_date_range')
    
    if not date_range or not date_range.get('has_dates_to_process'):
        logger.info("No dates for ad reports")
        return {'status': 'skipped'}
    
    start_date = date_range['start_date']
    end_date = date_range['end_date']
    
    logger.info(f"Processing ad reports from {start_date} to {end_date}")
    
    results = {
        'successful': [],
        'failed': []
    }
    
    try:
        # Process ad reports
        if call_amazon_ad_reports_api:
            logger.info("Processing ad reports...")
            result = call_amazon_ad_reports_api(start_date=start_date, end_date=end_date)
            logger.info(f"Ad reports completed: {result}")
            results['successful'].append('ad_reports')
        else:
            logger.warning("Ad reports function not available")
            
    except Exception as e:
        logger.error(f"Ad reports processing FAILED: {str(e)}")
        results['failed'].append({'error': str(e)})
    
    # Push results
    context['ti'].xcom_push(key='ad_report_results', value=results)
    
    logger.info(f"Ad reports complete. Successful: {len(results['successful'])}, Failed: {len(results['failed'])}")
    
    # Always return success - ad failures shouldn't stop the pipeline
    return results


def delay_execution(minutes: int):
    """Delay execution for the specified number of minutes."""
    logger.info(f"Delaying execution for {minutes} minutes")
    sleep(minutes * 60)
    logger.info(f"Delay of {minutes} minutes completed")

# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['ayushgoyal@bombayshavingcompany.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=4),
}

dag = DAG(
    'bsc_amazon',
    default_args=default_args,
    description='DAG for Amazon data pipeline with dynamic date processing and non-blocking ad reports',
    schedule_interval='0 3 * * *',  # Runs every day at 3 AM
    start_date=datetime(2024, 7, 30, 2, 0, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=['amazon', 'scraping', 'sales', 'reports', 'dynamic-dates'],
)

# =============================================================================
# TASK DEFINITIONS
# =============================================================================

# Start
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Calculate date range from database
calculate_date_range_task = PythonOperator(
    task_id='calculate_date_range',
    python_callable=calculate_date_range,
    provide_context=True,
    dag=dag,
)

# Branch based on whether there are dates to process
check_dates_branch = BranchPythonOperator(
    task_id='check_dates_to_process',
    python_callable=check_if_dates_to_process,
    provide_context=True,
    dag=dag,
)

# Skip to end if no dates
skip_to_end = EmptyOperator(
    task_id='skip_to_end',
    dag=dag,
)

# Process sales data for all dates
process_sales_task = PythonOperator(
    task_id='process_sales_data',
    python_callable=process_sales_data,
    provide_context=True,
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)


# Process ad reports (non-blocking)
process_ad_reports_task = PythonOperator(
    task_id='process_ad_reports',
    python_callable=process_ad_reports,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream fails
    dag=dag,
)


# Delay before DSR processing
delay_before_dsr_task = PythonOperator(
    task_id='delay_before_dsr',
    python_callable=delay_execution,
    op_kwargs={'minutes': 30},
    trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of ad report status
    dag=dag,
)

# DSR tasks
process_dsr_task = PythonOperator(
    task_id='process_dsr',
    python_callable=process_dsr,
    op_kwargs={
        'channel': 'Amazon',
    },
    dag=dag
)

generate_dsr_task = PythonOperator(
    task_id='generate_dsr',
    python_callable=generate_dsr,
    op_kwargs={
        'channel': 'Amazon',
        'devOrProd': '1',
    },
    dag=dag
)

process_central_dsr_dump_task = PythonOperator(
    task_id='process_central_dsr_dump',
    python_callable=process_central_dsr_dump,
    dag=dag
)

refresh_business_overview_task = PythonOperator(
    task_id='refresh_business_overview',
    python_callable=refresh_business_overview,
    dag=dag
)
# End
end = EmptyOperator(
    task_id='end',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# =============================================================================
# TASK DEPENDENCIES
# =============================================================================

# Main flow
start >> calculate_date_range_task >> check_dates_branch

# Branch: If dates to process - Sales flow
check_dates_branch >> process_sales_task >> delay_before_dsr_task >> process_dsr_task >> generate_dsr_task >> process_central_dsr_dump_task >> refresh_business_overview_task >> end

# Branch: If dates to process - Ad reports flow (parallel to sales)
check_dates_branch >> process_ad_reports_task >> delay_before_dsr_task
# Branch: If no dates to process
check_dates_branch >> skip_to_end >> end
