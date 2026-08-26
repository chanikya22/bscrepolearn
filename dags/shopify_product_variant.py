import json
from datetime import datetime, timedelta
import requests
from airflow import DAG
from pendulum import timezone
from environmentconfig import get_config
from utils.generic import download_insyt_token_from_s3
from plugins.operators.tracked_python_operator import (
    TrackedPythonOperator
)

ALERT_EMAILS = [
    "shashankbhushan@bombayshavingcompany.com"
]

local_tz = timezone("Asia/Kolkata")
SNAPSHOT_PATH = "/Home/Shopify-Product-Variants/Snapshot"


def trigger_product_variant_snapshot_task(run_id=None, tracker=None, **context):
    step_id = None
    url = None

    try:
        if tracker and run_id:
            step_id = tracker.start_pipeline_step(
                run_id=run_id,
                step_name="trigger_shopify_product_variant_snapshot"
            )

        credentials_path = get_config("bsc_credentials_path")

        with open(credentials_path, "r") as cf:
            credentials = json.load(cf)

        insyt_api_url = credentials.get("insyt-api-url", "").rstrip("/")
        url = f"{insyt_api_url}{SNAPSHOT_PATH}"

        token = download_insyt_token_from_s3()

        headers = {
            "accept": "text/plain",
            "Authorization": f"Bearer {token}"
        }

        print(f"Triggering Shopify Product Variant Snapshot API: {url}")

        response = requests.post(
            url,
            headers=headers,
            timeout=(10, 600)
        )

        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")

        response.raise_for_status()

        response_json = response.json()

        if not response_json.get("isSuccess", False):
            raise Exception(
                f"Shopify Product Variant Snapshot API failed: "
                f"{response_json.get('message', 'Unknown error')}"
            )

        print("Shopify Product Variant Snapshot API triggered successfully")

        if tracker and step_id:
            tracker.complete_pipeline_step(
                step_id=step_id,
                status="success",
                record_count=1,
                output_location=str(response_json)
            )

        return response_json

    except requests.exceptions.Timeout:
        error = f"Timed out while calling {url}"

    except requests.exceptions.HTTPError:
        error = (
            f"Shopify Product Variant Snapshot API failed with status code "
            f"{response.status_code}: {response.text}"
        )

    except requests.exceptions.RequestException as e:
        error = f"Failed to trigger Shopify Product Variant Snapshot API: {str(e)}"

    except Exception as e:
        error = str(e)

    if tracker and step_id:
        tracker.complete_pipeline_step(
            step_id=step_id,
            status="failed",
            error_message=error
        )

    raise Exception(error)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="shopify_product_variant",
    description="Shopify BSC + Bombae product variant snapshot via InsytAll",
    schedule_interval="0 12 * * *",  # 12:00 PM IST
    start_date=datetime(2026, 8, 21, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    default_args=default_args,
    tags=["snapshots", "shopify", "product-variants", "insyt"],
) as dag:

    product_variant_snapshot_task = TrackedPythonOperator(
        task_id="API_Product_Variant_Snapshot",
        python_callable=trigger_product_variant_snapshot_task,
        pipeline_name="bsc_product-variant-snapshot",
        client_id="bsc",
        data_type="bsc_product-variant-snapshot",
        is_first_task=True,
        is_last_task=True,
        failure_email_to=ALERT_EMAILS,
    )

    product_variant_snapshot_task
