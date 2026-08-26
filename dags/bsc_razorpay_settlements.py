from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.tracked_python_operator import (
    TrackedPythonOperator
)

from connections.bsc.razorpay.settelment.portal_login import (
    portal_login_download_report,
    upload_s3_task,
)
from connections.bsc.razorpay.settelment.extract_load import (
    load as load_settlement
)


ALERT_EMAILS = [
    "manish.p@bombayshavingcompany.com",
    # "tech@bombayshavingcompany.com"
]


def load_settlement_db(run_id=None, tracker=None, **context):
    return load_settlement("bsc")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="bsc_razorpay_settlements",
    description="Razorpay Shopify Settlement Report Pipeline",
    schedule="30 3 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["razorpay", "settlement", "shopify", "bsc"]
) as dag:

    start = EmptyOperator(task_id="start")

    extract_report_task = TrackedPythonOperator(
        task_id="extract_report",
        python_callable=portal_login_download_report,
        pipeline_name="razorpay_shopify_settlement",
        client_id="bsc",
        data_type="razorpay_shopify_settlement",
        is_first_task=True,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    upload_s3 = TrackedPythonOperator(
        task_id="upload_s3",
        python_callable=upload_s3_task,
        is_first_task=False,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    load_db = TrackedPythonOperator(
        task_id="load_db",
        python_callable=load_settlement_db,
        is_first_task=False,
        is_last_task=True,
        failure_email_to=ALERT_EMAILS,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_report_task >> upload_s3 >> load_db >> end