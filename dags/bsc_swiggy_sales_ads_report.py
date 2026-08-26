from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from plugins.operators.tracked_python_operator import (
    TrackedPythonOperator
)

from connections.insytscraping.swiggy.swiggy_sales_extract_load_report import (
    extract as extract_sales,
    upload_s3_task as upload_sales_s3,
    validate_s3_task as validate_sales_s3,
    api_upload_task as api_upload_sales,
    load_db_task as load_sales_db,
    validate_db_task as validate_sales_db
)

from connections.insytscraping.swiggy.swiggy_ads_extract_load_report import (
    extract as extract_ads,
    upload_s3_task as upload_ads_s3,
    validate_s3_task as validate_ads_s3,
    api_upload_task as api_upload_ads,
    load_db_task as load_ads_db,
    validate_db_task as validate_ads_db
)

from connections.insytscraping.swiggy.swiggy_process_dsr import (
    trigger_dsr_task,
    validate_process_dsr_db_task,
    trigger_dsr_summary_task,
    trigger_central_dsr_task,
    check_dsr_email)



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
    dag_id="swiggy_sales_ads_po_report",
    description="Swiggy Sales and Ads pipeline",
    schedule="30 2 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    tags=["swiggy", "sales", "ads", "bsc", "po"]
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="sales_group") as sales_group:

        extract_sales_task = TrackedPythonOperator(
            task_id="extract_sales",
            python_callable=extract_sales,
            pipeline_name="bsc_swiggy-report",
            client_id="bsc",
            data_type="bsc_swiggy-report",
            is_first_task=True,
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
        )

        (
            extract_sales_task
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
            pipeline_name="bsc_swiggy-report",
            client_id="bsc",
            data_type="bsc_swiggy-report",
            trigger_rule="all_done",
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
            is_last_task=False,
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

    trigger_dsr_api_task = TrackedPythonOperator(
        task_id="trigger_dsr_api",
        python_callable=trigger_dsr_task,
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    validate_process_dsr_task = TrackedPythonOperator(
            task_id="validate_process_dsr_db",
            python_callable=validate_process_dsr_db_task,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )
    
    trigger_dsr_summary_api_task = TrackedPythonOperator(
        task_id="trigger_dsr_summary",
        python_callable=trigger_dsr_summary_task,
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    # check_dsr_email_task = TrackedPythonOperator(
    #     task_id="check_dsr_email",
    #     python_callable=check_dsr_email,
    #     is_first_task=False,
    #     is_last_task=False,
    #     failure_email_to=ALERT_EMAILS,
    # )

    trigger_central_dsr_api_task = TrackedPythonOperator(
        task_id="trigger_central_dsr_api",
        python_callable=trigger_central_dsr_task,
        is_first_task=False,
        is_last_task=True,
        failure_email_to=ALERT_EMAILS,
    )


    end = EmptyOperator(task_id="end")


    start >> sales_group >> ads_group

    # DSR chain runs only if BOTH sales and ads fully succeeded.
    # Depending on both groups (default all_success rule) means a run where
    # sales failed but ads passed will NOT trigger the DSR chain.
    [sales_group, ads_group] >> trigger_dsr_api_task

    (
        trigger_dsr_api_task
        >> validate_process_dsr_task
        >> trigger_dsr_summary_api_task
    )

    # Central DSR runs only if process_dsr succeeded (validate_process_dsr_task),
    # and only after trigger_dsr_summary has completed.
    [validate_process_dsr_task, trigger_dsr_summary_api_task] >> trigger_central_dsr_api_task >> end