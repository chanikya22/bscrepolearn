import json
from datetime import datetime, timedelta

import requests
from airflow import DAG

from environmentconfig import get_config
from utils.generic import download_insyt_token_from_s3

from plugins.operators.tracked_python_operator import (
    TrackedPythonOperator
)

ALERT_EMAILS = [
    "manish.p@bombayshavingcompany.com"
]


def trigger_refresh_business_overview_task(run_id=None, tracker=None, **context):

    step_id = None

    try:
        if tracker and run_id:
            step_id = tracker.start_pipeline_step(
                run_id=run_id,
                step_name="trigger_refresh_business_overview"
            )

        credentials_path = get_config("bsc_credentials_path")

        with open(credentials_path, "r") as cf:
            credentials = json.load(cf)

        insyt_api_url = credentials.get("insyt-api-url")

        url = f"{insyt_api_url}/View/RefreshBusinessOverview"

        token = download_insyt_token_from_s3()

        headers = {
            "accept": "text/plain",
            "Authorization": f"Bearer {token}"
        }

        print(f"Triggering Refresh Business Overview API: {url}")

        response = requests.post(
            url,
            headers=headers,
            timeout=(10, 300)
        )

        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")

        response.raise_for_status()

        response_json = response.json()

        if not response_json.get("isSuccess", False):
            raise Exception(
                f"Refresh Business Overview API failed: "
                f"{response_json.get('message', 'Unknown error')}"
            )

        print("✓ Refresh Business Overview API triggered successfully")

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
            f"Refresh Business Overview API failed with status code "
            f"{response.status_code}: {response.text}"
        )

    except requests.exceptions.RequestException as e:
        error = f"Failed to trigger Refresh Business Overview API: {str(e)}"

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
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="refresh_business_overview",
    description="Refresh Business Overview materialized view every 15 minutes",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 8, 12),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args=default_args,
    tags=["insyt", "refresh", "business_overview", "bsc"],
) as dag:

    refresh_business_overview_task = TrackedPythonOperator(
        task_id="trigger_refresh_business_overview",
        python_callable=trigger_refresh_business_overview_task,
        pipeline_name="bsc_refresh-business-overview",
        client_id="bsc",
        data_type="bsc_refresh-business-overview",
        is_first_task=True,
        is_last_task=True,
        failure_email_to=ALERT_EMAILS,
    )

    refresh_business_overview_task