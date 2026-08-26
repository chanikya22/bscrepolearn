import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.operators.tracked_python_operator import (
    TrackedPythonOperator
)

from connections.insytscraping.amazon.amazon_kk_rk_reports import (
    Database_missing_date_check,
    extract as extract_sales,
    upload_s3_task as upload_sales_s3,
    validate_s3_task as validate_sales_s3,
    api_upload_task as api_upload_sales,
    load_db_task as load_sales_db,
    validate_db_task as validate_sales_db,
)

from connections.insytscraping.amazon.amazon_process_dsr import (
    trigger_process_dsr_task,
    validate_process_dsr_db_task,
    trigger_dsr_summary_task,
    trigger_central_dsr_task,
)

from connections.insytscraping.amazon.amazon_ads_report import (
    process_ad_reports_task as process_ads_reports,
    verify_ad_reports_task as verify_ads_reports,
)

from connections.insytscraping.amazon.amazon_fba_reports import (
    Database_missing_date_check as fba_database_missing_date_check,
    extract as extract_fba,
    upload_s3_task as upload_fba_s3,
    validate_s3_task as validate_fba_s3,
    api_upload_task as api_upload_fba,
    load_db_task as load_fba_db,
    validate_db_task as validate_fba_db,
)

ALERT_EMAILS = [
    "manish.p@bombayshavingcompany.com"
]

FBA_START_DELAY_SECONDS = 120   # kk_rk and fba share the same OTP secret;
                                 # staggering their logins keeps them from
                                 # hitting Amazon's OTP step at the same
                                 # moment.


def delay_fba_start(**context):
    print(
        f"Delaying fba_group's start by {FBA_START_DELAY_SECONDS}s so its "
        f"login doesn't land on the OTP step at the same time as kk_rk_group's."
    )
    time.sleep(FBA_START_DELAY_SECONDS)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="amazon_DSR",
    description="Amazon Vendor Central (kk/rk) sales report pipeline",
    schedule="45 2 * * *",
    start_date=datetime(2026, 8, 17),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args=default_args,
    tags=["amazon", "sales", "bsc"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -----------------------------
    # ADS PIPELINE (non-blocking — failures here never stop sales/DSR)
    # -----------------------------
    with TaskGroup(group_id="ads_group") as ads_group:

        process_ads_reports_task = TrackedPythonOperator(
            task_id="process_ad_reports",
            python_callable=process_ads_reports,
            is_first_task=True,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        verify_ads_reports_task = TrackedPythonOperator(
            task_id="verify_ad_reports",
            python_callable=verify_ads_reports,
            trigger_rule="all_done",
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        process_ads_reports_task >> verify_ads_reports_task

    # -----------------------------
    # SALES PIPELINE (kk + rk, one login session)
    # -----------------------------
    with TaskGroup(group_id="kk_rk_group") as kk_rk_group:

        database_missing_date_check_task = TrackedPythonOperator(
            task_id="database_missing_date_check",
            python_callable=Database_missing_date_check,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        extract_sales_task = TrackedPythonOperator(
            task_id="extract_sales",
            python_callable=extract_sales,
            pipeline_name="bsc_amazon-kk-rk-sales-report",
            client_id="bsc",
            data_type="bsc_amazon-kk-rk-sales-report",
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
        )

        (
            database_missing_date_check_task
            >> extract_sales_task
            >> upload_sales_s3_task
            >> validate_sales_s3_task
            >> api_upload_sales_task
            >> load_sales_db_task
            >> validate_sales_db_task
        )

    delay_before_fba_task = PythonOperator(
        task_id="delay_before_fba",
        python_callable=delay_fba_start,
    )

    # -----------------------------
    # FBA PIPELINE
    # -----------------------------
    with TaskGroup(group_id="fba_group") as fba_group:

        fba_database_missing_date_check_task = TrackedPythonOperator(
            task_id="database_missing_date_check",
            python_callable=fba_database_missing_date_check,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        extract_fba_task = TrackedPythonOperator(
            task_id="extract_fba",
            python_callable=extract_fba,
            pipeline_name="bsc_amazon-fba-report",
            client_id="bsc",
            data_type="bsc_amazon-fba-report",
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        upload_fba_s3_task = TrackedPythonOperator(
            task_id="upload_s3",
            python_callable=upload_fba_s3,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        validate_fba_s3_task = TrackedPythonOperator(
            task_id="validate_s3",
            python_callable=validate_fba_s3,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        api_upload_fba_task = TrackedPythonOperator(
            task_id="api_upload",
            python_callable=api_upload_fba,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        load_fba_db_task = TrackedPythonOperator(
            task_id="load_db",
            python_callable=load_fba_db,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        validate_fba_db_task = TrackedPythonOperator(
            task_id="validate_db",
            python_callable=validate_fba_db,
            is_first_task=False,
            is_last_task=False,
            failure_email_to=ALERT_EMAILS,
        )

        (
            fba_database_missing_date_check_task
            >> extract_fba_task
            >> upload_fba_s3_task
            >> validate_fba_s3_task
            >> api_upload_fba_task
            >> load_fba_db_task
            >> validate_fba_db_task
        )

    trigger_dsr_api_task = TrackedPythonOperator(
        task_id="trigger_dsr_api",
        python_callable=trigger_process_dsr_task,
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

    trigger_central_dsr_api_task = TrackedPythonOperator(
        task_id="trigger_central_dsr",
        python_callable=trigger_central_dsr_task,
        is_first_task=False,
        is_last_task=True,
        failure_email_to=ALERT_EMAILS,
    )

    end = EmptyOperator(task_id="end")

    # ads, kk_rk, and fba run in parallel — ads is just API calls (I/O
    # wait on our side, 5-30 min per report), kk_rk and fba are separate
    # browser-scraping sessions (different portals, nothing shared
    # locally) — except both log in with the same OTP secret, so fba's
    # start is staggered 2 minutes behind kk_rk's to keep their OTP entry
    # from landing in the same moment.
    start >> ads_group
    start >> kk_rk_group
    start >> delay_before_fba_task >> fba_group

    # DSR only fires if ALL THREE actually succeeded — depending on all
    # of them (default all_success trigger rule) means a run where any
    # one group failed will NOT trigger DSR.
    [ads_group, kk_rk_group, fba_group] >> trigger_dsr_api_task

    trigger_dsr_api_task >> validate_process_dsr_task >> trigger_dsr_summary_api_task

    # trigger_central_dsr depends directly on validate_process_dsr_task's
    # success (not just transitively through trigger_dsr_summary_api_task),
    # while the edge from trigger_dsr_summary_api_task keeps it running
    # after that step — both direct dependencies must succeed (default
    # all_success trigger rule) before this fires.
    [validate_process_dsr_task, trigger_dsr_summary_api_task] >> trigger_central_dsr_api_task >> end
 