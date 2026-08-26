from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow")

from airflow import DAG
from connections._100days.neurogum_qcom.soda_health_check.run_soda import (
    run_scan as run_soda_scan,
)
from plugins.operators.tracked_python_operator import TrackedPythonOperator


ALERT_EMAILS = [
    "shashankbhushan@bombayshavingcompany.com",
    "shishank@bombayshavingcompany.com",
]


def data_freshness_checks():
    run_soda_scan(
        "freshness_audit",
        "audit_db",
        ["checks/audit_db/data_freshness_checks.yaml"],
    )


def blinkit_checks():
    run_soda_scan(
        "blinkit_checks",
        "warehouse_db",
        ["checks/warehouse_db/blinkit_checks.yaml"],
    )


def swiggy_checks():
    run_soda_scan(
        "swiggy_checks",
        "warehouse_db",
        ["checks/warehouse_db/swiggy_checks.yaml"],
    )


def zepto_checks():
    run_soda_scan(
        "zepto_checks",
        "warehouse_db",
        ["checks/warehouse_db/zepto_checks.yaml"],
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="neurogum_qcom_soda_checks",
    description="Neurogum QCom pipeline health checks",
    schedule_interval="0 3 * * *",  # Daily at 8:30 AM IST
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "neurogum", "qcom", "health-check"],
)

data_freshness_task = TrackedPythonOperator(
    task_id="data_freshness_checks",
    python_callable=data_freshness_checks,
    pipeline_name="neurogum-qcom-soda-checks",
    client_id="neurogum",
    data_type="health-check",
    is_first_task=True,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule="all_done",
    dag=dag,
)

zepto_checks_task = TrackedPythonOperator(
    task_id="zepto_sales_checks",
    python_callable=zepto_checks,
    pipeline_name="neurogum-qcom-soda-checks",
    client_id="neurogum",
    data_type="health-check",
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule="all_done",
    dag=dag,
)

blinkit_checks_task = TrackedPythonOperator(
    task_id="blinkit_sales_checks",
    python_callable=blinkit_checks,
    pipeline_name="neurogum-qcom-soda-checks",
    client_id="neurogum",
    data_type="health-check",
    is_first_task=False,
    is_last_task=False,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule="all_done",
    dag=dag,
)

swiggy_checks_task = TrackedPythonOperator(
    task_id="swiggy_sales_checks",
    python_callable=swiggy_checks,
    pipeline_name="neurogum-qcom-soda-checks",
    client_id="neurogum",
    data_type="health-check",
    is_first_task=False,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule="all_done",
    dag=dag,
)

data_freshness_task >> zepto_checks_task >> blinkit_checks_task >> swiggy_checks_task
