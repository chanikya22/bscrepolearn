from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")

from connections._100days.neurogum_qcom.blinkit.extract_load import load as blinkit_load
from connections._100days.neurogum_qcom.zepto.extract_load import load as zepto_load
from connections._100days.neurogum_qcom.swiggy.extract_load import load as swiggy_load
from plugins.operators.tracked_python_operator import TrackedPythonOperator
from utils.postgresconnector_v3 import PostgresConnector


def upsert_sales_summary():
    postgres = PostgresConnector(db_prefix="warehouse_")
    query = """CALL neurogum.upsert_qcom_sales_summary(); """
    postgres.execute_query(query)


ALERT_EMAILS = [
    "shashankbhushan@bombayshavingcompany.com",
    "shishank@bombayshavingcompany.com",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="_100days_neurogum_qcom_sales",
    description="pipeline to send sales data of neurogum",
    schedule_interval="30 2 * * *",  # Daily at 8:00 AM IST
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["Blinkit", "Zepto", "Swiggy", "neurogum", "100days", "qcom"],
)

start = EmptyOperator(task_id="start", dag=dag)

zepto_sales_load_task = TrackedPythonOperator(
    task_id="zepto_sales_load",
    python_callable=zepto_load,
    op_kwargs={"brand": "neurogum"},
    pipeline_name="neurogum-zepto-sales",
    client_id="neurogum",
    data_type="neurogum_zepto_sales",
    is_first_task=True,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule="all_done",
    dag=dag,
)

swiggy_sales_load_task = TrackedPythonOperator(
    task_id="swiggy_sales_load",
    python_callable=swiggy_load,
    op_kwargs={"brand": "neurogum"},
    pipeline_name="neurogum-swiggy-sales",
    client_id="neurogum",
    data_type="neurogum_swiggy_sales",
    is_first_task=True,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule="all_done",
    dag=dag,
)

blinkit_sales_load_task = TrackedPythonOperator(
    task_id="blinkit_sales_load",
    python_callable=blinkit_load,
    op_kwargs={"brand": "neurogum"},
    pipeline_name="neurogum-blinkit-sales",
    client_id="neurogum",
    data_type="neurogum_blinkit_sales",
    is_first_task=True,
    is_last_task=True,
    failure_email_to=ALERT_EMAILS,
    success_email_to=ALERT_EMAILS,
    trigger_rule="all_done",
    dag=dag,
)

upsert_sales_summary_task = PythonOperator(
    task_id="upsert_sales_summary_task",
    python_callable=upsert_sales_summary,
    dag=dag,
)

end_task = DummyOperator(
    task_id="end_pipeline",
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

start >> zepto_sales_load_task >> upsert_sales_summary_task >> end_task
start >> swiggy_sales_load_task >> upsert_sales_summary_task >> end_task
start >> blinkit_sales_load_task >> upsert_sales_summary_task >> end_task
