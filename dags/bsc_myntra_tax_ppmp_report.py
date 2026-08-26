from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.tracked_python_operator import (
    TrackedPythonOperator
)

from connections.insytscraping.myntra.myntra_tax_ppmp_report import (
    extract as extract_tax_ppmp,
    api_upload_task as api_upload_tax_ppmp,
)


ALERT_EMAILS = [
    "shashankbhushan@bombayshavingcompany.com",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="myntra_tax_ppmp",
    description="Download Myntra PPMP sales file and upload tax report",
    schedule="30 4 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["myntra", "tax", "ppmp", "bsc"]
) as dag:

    start = EmptyOperator(task_id="start")

    extract_tax_ppmp_task = TrackedPythonOperator(
        task_id="extract_tax_ppmp",
        python_callable=extract_tax_ppmp,
        pipeline_name="myntra_tax_ppmp-report",
        client_id="bsc",
        data_type="myntra_tax_ppmp-report",
        is_first_task=True,
        is_last_task=False,
        failure_email_to=ALERT_EMAILS,
    )

    api_upload_tax_ppmp_task = TrackedPythonOperator(
        task_id="api_upload",
        python_callable=api_upload_tax_ppmp,
        is_first_task=False,
        is_last_task=True,
        failure_email_to=ALERT_EMAILS,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_tax_ppmp_task >> api_upload_tax_ppmp_task >> end
