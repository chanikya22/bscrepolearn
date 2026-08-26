from datetime import datetime, timedelta
import sys
import pendulum
sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from plugins.operators.tracked_python_operator import TrackedPythonOperator
from utils.postgresconnector_v3 import PostgresConnector

from connections.insytscraping.blinkit.blinkit_po_report import (
    extract as extract_blinkit_po,
    validate_s3_task as validate_blinkit_po_s3,
    api_upload as api_upload_blinkit_po,
    load_db as load_blinkit_po_db,
)

from connections.insytscraping.swiggy.swiggy_po_extract_load_report import (
    extract as extract_swiggy_po,
    validate_s3_file as validate_swiggy_po_s3,
    api_upload as api_upload_swiggy_po,
    load_db as load_po_swiggy_db,
)

from connections.insytscraping.zepto.zepto_po_report import (
    extract as extract_zepto_po,
    validate_s3_task as validate_zepto_po_s3,
    api_upload as api_upload_zepto_po,
    load_db as load_zepto_po_db,
)

ALERT_EMAILS = [
    'lakshay@bombayshavingcompany.com',
    'shishank@bombayshavingcompany.com',
    'sujal@bombayshavingcompany.com',
]

IST = pendulum.timezone("Asia/Kolkata")

def refresh_qcom_fillrate():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """
                select * from bsc.refresh_qcom_fill_rate()
            """
    postgres.execute_query(query)


# ---------------------------------------------------------------------------
# Helper: builds one extract -> validate -> api_upload -> load_db task group
# ---------------------------------------------------------------------------
def build_po_group(platform, extract_fn, validate_fn, api_upload_fn, load_fn):
    with TaskGroup(group_id=f"{platform}_po_group") as group:
        extract_task = TrackedPythonOperator(
            task_id="extract_po",
            python_callable=extract_fn,
            pipeline_name="po_automation_pipeline",
            client_id="bsc",
            data_type="po_automation_pipeline",
            is_first_task=True,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        validate_s3_task = TrackedPythonOperator(
            task_id="validate_s3",
            python_callable=validate_fn,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        api_upload_task = TrackedPythonOperator(
            task_id="api_upload",
            python_callable=api_upload_fn,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        load_db_task = TrackedPythonOperator(
            task_id="load_db",
            python_callable=load_fn,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        extract_task >> validate_s3_task >> api_upload_task >> load_db_task

    return group


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='bsc_qcom_po_automation',
    description='pipeline to send dsr',
    schedule_interval='0 1-23/2 * * *',  # Every 2h, 12 runs/day IST (11,13,...,23,1,3,...,9)
    default_args=default_args,
    start_date=datetime(2025, 12, 17, tzinfo=IST),
    catchup=False,
    tags=['Blinkit', 'Zepto', 'Swiggy', 'po', 'bsc'],
)

with dag:
    start = EmptyOperator(task_id='start')

    blinkit_po_group = build_po_group(
        "blinkit",
        extract_blinkit_po,
        validate_blinkit_po_s3,
        api_upload_blinkit_po,
        load_blinkit_po_db,
    )

    swiggy_po_group = build_po_group(
        "swiggy",
        extract_swiggy_po,
        validate_swiggy_po_s3,
        api_upload_swiggy_po,
        load_po_swiggy_db,
    )

    zepto_po_group = build_po_group(
        "zepto",
        extract_zepto_po,
        validate_zepto_po_s3,
        api_upload_zepto_po,
        load_zepto_po_db,
    )

    refresh_qcom_fillrate_task = TrackedPythonOperator(
        task_id='refresh_qcom_fillrate',
        python_callable=refresh_qcom_fillrate,
        pipeline_name="po_automation_pipeline",
        client_id="bsc",
        data_type="po_automation_pipeline",
        is_first_task=False,
        is_last_task=True,
        failure_email_to=ALERT_EMAILS,
        success_email_to=ALERT_EMAILS,
        trigger_rule='all_done',  # run even if some groups fail
    )

    end_task = EmptyOperator(
        task_id='end_pipeline',
        trigger_rule='none_failed_min_one_success',
    )

    (
        start
        >> [blinkit_po_group, swiggy_po_group, zepto_po_group]
        >> refresh_qcom_fillrate_task
        >> end_task
    )