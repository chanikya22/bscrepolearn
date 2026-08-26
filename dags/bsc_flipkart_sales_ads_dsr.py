from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from plugins.operators.tracked_python_operator import (
    TrackedPythonOperator
)

from connections.insytscraping.flipkart.flipkart_earn_more_report import (
    request_earn_more_report,
    check_listings_report_ready_task,
    download_listings_report,
    upload_s3_task as upload_sales_s3,
    validate_s3_task as validate_sales_s3,
    api_upload_task as api_upload_sales,
    load_db_task as load_sales_db,
    validate_db_task as validate_sales_db
)

from connections.insytscraping.flipkart.flipkart_pla_pca_report import (
    extract as extract_ads,
    upload_s3_task as upload_ads_s3,
    validate_s3_task as validate_ads_s3,
    api_upload_task as api_upload_ads,
    load_db_task as load_ads_db,
    validate_db_task as validate_ads_db
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
    dag_id="flipkart_earn_more_pla_pca_report",
    description="Flipkart Earn More Sales pipeline",
    schedule="30 5 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    default_args=default_args,
    tags=["flipkart", "sales", "ads", "bsc"]
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="sales_group") as sales_group:

        request_report_task = TrackedPythonOperator(
            task_id="request_report",
            python_callable=request_earn_more_report,
            pipeline_name="flipkart_sales_ads_dsr",
            client_id="bsc",
            data_type="flipkart_sales_ads_dsr",
            is_first_task=True,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        wait_for_report_ready = TrackedPythonOperator(
            task_id="wait_for_report_ready",
            python_callable=check_listings_report_ready_task,
            retries=8,
            retry_delay=timedelta(minutes=30),
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        download_report_task = TrackedPythonOperator(
            task_id="download_report",
            python_callable=download_listings_report,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        upload_sales_s3_task = TrackedPythonOperator(
            task_id="upload_s3",
            python_callable=upload_sales_s3,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        validate_sales_s3_task = TrackedPythonOperator(
            task_id="validate_s3",
            python_callable=validate_sales_s3,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        api_upload_sales_task = TrackedPythonOperator(
            task_id="api_upload",
            python_callable=api_upload_sales,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        load_sales_db_task = TrackedPythonOperator(
            task_id="load_db",
            python_callable=load_sales_db,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        validate_sales_db_task = TrackedPythonOperator(
            task_id="validate_db",
            python_callable=validate_sales_db,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
            success_email_to=ALERT_EMAILS,
        )

        (
            request_report_task
            >> wait_for_report_ready
            >> download_report_task
            >> upload_sales_s3_task
            >> validate_sales_s3_task
            >> api_upload_sales_task
            >> load_sales_db_task
            >> validate_sales_db_task
        )

    with TaskGroup(group_id="ads_group") as ads_group:

        extract_ads_task = TrackedPythonOperator(
            task_id="extract_ads",
            python_callable=extract_ads,
            pipeline_name="flipkart_sales_ads_dsr",
            client_id="bsc",
            data_type="flipkart_sales_ads_dsr",
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        upload_ads_s3_task = TrackedPythonOperator(
            task_id="upload_s3",
            python_callable=upload_ads_s3,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        validate_ads_s3_task = TrackedPythonOperator(
            task_id="validate_s3",
            python_callable=validate_ads_s3,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        api_upload_ads_task = TrackedPythonOperator(
            task_id="api_upload",
            python_callable=api_upload_ads,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        load_ads_db_task = TrackedPythonOperator(
            task_id="load_db",
            python_callable=load_ads_db,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        validate_ads_db_task = TrackedPythonOperator(
            task_id="validate_db",
            python_callable=validate_ads_db,
            is_first_task=False,
            is_last_task=True,
            failure_email_to=ALERT_EMAILS,
            success_email_to=ALERT_EMAILS,
        )

        (
            extract_ads_task
            >> upload_ads_s3_task
            >> validate_ads_s3_task
            >> api_upload_ads_task
            >> load_ads_db_task
            >> validate_ads_db_task
        )

    end = EmptyOperator(task_id="end")

    start >> sales_group >> ads_group >> end