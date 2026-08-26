from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.tracked_python_operator import (
    TrackedPythonOperator
)

from connections.insytscraping.nykaa.sales.extract import (
    extract_sales
)

from connections.insytscraping.nykaa.sales.load import (
    validate_s3_task as validate_sales_s3,
    load_task as load_sales,
    load_db as load_sales_db,
    validate_db_task as validate_sales_db
)

from connections.insytscraping.nykaa.nykaa_process_dsr import (
    trigger_dsr_task,
    validate_process_dsr_db_task,
    trigger_dsr_summary_task,
    trigger_central_dsr_task,
    check_dsr_email
)


ALERT_EMAILS = [
    "manish.p@bombayshavingcompany.com",
    "tech@bombayshavingcompany.com"
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="bsc_nykaa_sales_dsr",
    description="Nykaa Sales and DSR pipeline",
    schedule="30 2 * * *",   # 8:00 AM (IST) daily
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    tags=["nykaa", "sales", "dsr", "bsc"]
) as dag:

    start = EmptyOperator(task_id="start")

    extract_sales_task = TrackedPythonOperator(
        task_id="extract_sales",
        python_callable=extract_sales,
        pipeline_name="bsc_nykaa-sales-report",
        client_id="bsc",
        data_type="bsc_nykaa-sales-report",
        is_first_task=True,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    validate_sales_s3_task = TrackedPythonOperator(
        task_id="validate_s3",
        python_callable=validate_sales_s3,
        pipeline_name="bsc_nykaa-sales-report",
        client_id="bsc",
        data_type="bsc_nykaa-sales-report",
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    load_sales_task = TrackedPythonOperator(
        task_id="api_upload",
        python_callable=load_sales,
        pipeline_name="bsc_nykaa-sales-report",
        client_id="bsc",
        data_type="bsc_nykaa-sales-report",
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    load_sales_db_task = TrackedPythonOperator(
        task_id="load_db",
        python_callable=load_sales_db,
        pipeline_name="bsc_nykaa-sales-report",
        client_id="bsc",
        data_type="bsc_nykaa-sales-report",
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    validate_sales_db_task = TrackedPythonOperator(
        task_id="validate_db",
        python_callable=validate_sales_db,
        pipeline_name="bsc_nykaa-sales-report",
        client_id="bsc",
        data_type="bsc_nykaa-sales-report",
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    trigger_dsr_api_task = TrackedPythonOperator(
        task_id="trigger_dsr_api",
        python_callable=trigger_dsr_task,
        pipeline_name="bsc_nykaa-sales-report",
        client_id="bsc",
        data_type="bsc_nykaa-sales-report",
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    validate_process_dsr_task = TrackedPythonOperator(
        task_id="validate_process_dsr_db",
        python_callable=validate_process_dsr_db_task,
        pipeline_name="bsc_nykaa-sales-report",
        client_id="bsc",
        data_type="bsc_nykaa-sales-report",
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    trigger_dsr_summary_api_task = TrackedPythonOperator(
        task_id="trigger_dsr_summary",
        python_callable=trigger_dsr_summary_task,
        pipeline_name="bsc_nykaa-sales-report",
        client_id="bsc",
        data_type="bsc_nykaa-sales-report",
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    trigger_central_dsr_api_task = TrackedPythonOperator(
        task_id="trigger_central_dsr_api",
        python_callable=trigger_central_dsr_task,
        pipeline_name="bsc_nykaa-sales-report",
        client_id="bsc",
        data_type="bsc_nykaa-sales-report",
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    check_dsr_email_task = TrackedPythonOperator(
        task_id="check_dsr_email",
        python_callable=check_dsr_email,
        pipeline_name="bsc_nykaa-sales-report",
        client_id="bsc",
        data_type="bsc_nykaa-sales-report",
        is_first_task=False,
        is_last_task=True,
        failure_email_to=ALERT_EMAILS,
        success_email_to=ALERT_EMAILS,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> extract_sales_task
        >> validate_sales_s3_task
        >> load_sales_task
        >> load_sales_db_task
        >> validate_sales_db_task
        >> trigger_dsr_api_task
        >> validate_process_dsr_task
        >> trigger_dsr_summary_api_task
        >> trigger_central_dsr_api_task
        >> check_dsr_email_task
        >> end
    )