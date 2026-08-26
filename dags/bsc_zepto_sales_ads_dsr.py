from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from plugins.operators.tracked_python_operator import (
    TrackedPythonOperator
)

from connections.insytscraping.zepto.zepto_sales_report import (
    extract as extract_sales,
    upload_s3_task as upload_sales_s3,
    validate_s3_task as validate_sales_s3,
    api_upload_task as api_upload_sales,
    validate_db_task as validate_sales_db,
)

from connections.insytscraping.zepto.zepto_ads_report import (
    Database_missing_date_check,
    extract as extract_ads,
    upload_s3_task as upload_ads_s3,
    validate_s3_task as validate_ads_s3,
    api_upload_task as api_upload_ads,
    validate_db_task as validate_ads_db,
)

from connections.insytscraping.zepto.zepto_process_dsr import (
    trigger_dsr_task,
    validate_process_dsr_db_task,
    trigger_dsr_summary_task,
    trigger_central_dsr_task,
    check_dsr_email)

from connections.insytscraping.zepto.zepto_inventory_report import (
    extract as extract_inventory,
    validate_s3_task as validate_inventory_s3,
    api_upload as api_upload_inventory,
    load_db as load_inventory_db,
)

ALERT_EMAILS = [
    "manish.p@bombayshavingcompany.com"
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="zepto_DSR",
    description="Zepto Sales and Ads pipeline",
    schedule="10 3 * * *",
    start_date=datetime(2026, 7, 3),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["zepto", "sales", "ads", "bsc"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -----------------------------
    # SALES PIPELINE
    # -----------------------------
    with TaskGroup(group_id="sales_group") as sales_group:

        extract_sales_task = TrackedPythonOperator(
            task_id="extract_sales",
            python_callable=extract_sales,
            pipeline_name="bsc_zepto-sales-report",
            client_id="bsc",
            data_type="bsc_zepto-sales-report",
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
            >> validate_sales_db_task
        )

    # -----------------------------
    # ADS PIPELINE
    # -----------------------------
    with TaskGroup(group_id="ads_group") as ads_group:

        database_missing_date_check_task = TrackedPythonOperator(
            task_id="database_missing_date_check",
            python_callable=Database_missing_date_check,
            trigger_rule="all_done",
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        extract_ads_task = TrackedPythonOperator(
            task_id="extract_ads",
            python_callable=extract_ads,
            pipeline_name="bsc_zepto-ads-report",
            client_id="bsc",
            data_type="bsc_zepto-ads-report",
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


        validate_ads_db_task = TrackedPythonOperator(
            task_id="validate_db",
            python_callable=validate_ads_db,
            is_last_task=False,
            is_first_task=False,
            failure_email_to=ALERT_EMAILS,
            success_email_to=ALERT_EMAILS,
        )

        (
            database_missing_date_check_task
            >> extract_ads_task
            >> upload_ads_s3_task
            >> validate_ads_s3_task
            >> api_upload_ads_task
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
        is_last_task=False,          # was True
        failure_email_to=ALERT_EMAILS,
    )

    # check_dsr_email_task = TrackedPythonOperator(
    #     task_id="check_dsr_email",
    #     python_callable=check_dsr_email,
    #     is_first_task=False,
    #     is_last_task=True,
    #     failure_email_to=ALERT_EMAILS,
    # )

    trigger_central_dsr_api_task = TrackedPythonOperator(
        task_id="trigger_central_dsr_api",
        python_callable=trigger_central_dsr_task,
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    with TaskGroup(
        group_id="inventory_group"
    ) as inventory_group:

        extract_inventory_task = TrackedPythonOperator(
            task_id="extract_inventory",
            python_callable=extract_inventory,
            pipeline_name="bsc_zepto-inventory-report",
            client_id="bsc",
            data_type="bsc_zepto-inventory-report",
            trigger_rule="all_done",   # runs whether upstream succeeded or failed
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        validate_inventory_s3_task = TrackedPythonOperator(
            task_id="validate_s3",
            python_callable=validate_inventory_s3,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        api_upload_inventory_task = TrackedPythonOperator(
            task_id="api_upload",
            python_callable=api_upload_inventory,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        load_inventory_db_task = TrackedPythonOperator(
            task_id="load_db",
            python_callable=load_inventory_db,
            is_first_task=False,
            is_last_task=True,
            failure_email_to=ALERT_EMAILS,
        )

        (
            extract_inventory_task
            >> validate_inventory_s3_task
            >> api_upload_inventory_task
            >> load_inventory_db_task
        )

    end = EmptyOperator(task_id="end")

    
    start >> sales_group >> ads_group
    # DSR chain runs only if BOTH groups fully succeeded.
    # Depending on both groups (with default all_success rule) means a run where
    # sales failed but ads passed will NOT trigger DSR.
    [sales_group, ads_group] >> trigger_dsr_api_task

    (
        trigger_dsr_api_task
        >> validate_process_dsr_task
        >> trigger_dsr_summary_api_task
    )

    # Central DSR runs only if process_dsr succeeded (validate_process_dsr_task),
    # and only after trigger_dsr_summary has completed.
    [validate_process_dsr_task, trigger_dsr_summary_api_task] >> trigger_central_dsr_api_task

    # Inventory runs LAST, after the DSR summary, but ALWAYS runs
    # (all_done on its entry task) regardless of upstream success/failure.
    trigger_dsr_summary_api_task >> inventory_group >> end
    trigger_central_dsr_api_task >> end