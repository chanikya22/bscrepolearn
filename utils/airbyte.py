import os
import requests
import time
from dotenv import load_dotenv
import environmentconfig
load_dotenv()

# Fix: use os.getenv() to read values loaded from .env by dotenv
AIRBYTE_HOST = os.getenv("AIRBYTE_HOST")
USERNAME = os.getenv("airbyte_USER")
PASSWORD = os.getenv("airbyte_PASSWORD")


def trigger_connection(connection_id):
    """
    Triggers an Airbyte connection sync via the Airbyte API.

    Sends a POST request to the /connections/sync endpoint with the given
    connection ID. On success, extracts and returns the job ID from the
    response so the caller can poll for completion.

    Args:
        connection_id (str): The UUID of the Airbyte connection to sync.
                             e.g. "b106efa5-8cf3-4f48-a56b-1cad5e994a57"

    Returns:
        int: The job ID of the triggered sync job.

    Raises:
        requests.HTTPError: If the API call fails (non-2xx response).
    """
    url = f"{AIRBYTE_HOST}/api/v1/connections/sync"

    payload = {
        "connectionId": connection_id
    }

    response = requests.post(url, json=payload, auth=(USERNAME, PASSWORD))
    response.raise_for_status()

    job_id = response.json()["job"]["id"]
    print(f"Triggered Airbyte job: {job_id}")

    return job_id


def get_job_status(job_id):
    """
    Fetches the current status of an Airbyte sync job.

    Sends a POST request to the /jobs/get endpoint with the given job ID
    and returns the job's status string (e.g. "running", "succeeded",
    "failed", "cancelled").

    Args:
        job_id (int): The Airbyte job ID returned by trigger_connection().

    Returns:
        str: The current status of the job.

    Raises:
        requests.HTTPError: If the API call fails (non-2xx response).
    """
    url = f"{AIRBYTE_HOST}/api/v1/jobs/get"

    payload = {
        "id": job_id
    }

    response = requests.post(url, json=payload, auth=(USERNAME, PASSWORD))
    response.raise_for_status()

    return response.json()["job"]["status"]


def run_airbyte_connection(connection_id, poll_interval=10):
    """
    Triggers an Airbyte sync and blocks until the job reaches a terminal state.

    Internally calls trigger_connection() to kick off the sync, then polls
    get_job_status() every `poll_interval` seconds until the job either
    succeeds, fails, or is cancelled. Designed to be used as an Airflow task
    or any orchestration step where you need synchronous behaviour.

    Args:
        connection_id (str): The UUID of the Airbyte connection to sync.
        poll_interval (int): Seconds to wait between status polls. Default 10.

    Returns:
        bool: True if the sync completed successfully.

    Raises:
        Exception: If the sync job fails or is cancelled, so that Airflow
                   (or any caller) can mark the task as failed.
    """

    job_id = trigger_connection(connection_id)

    while True:
        status = get_job_status(job_id)
        print(f"Airbyte job {job_id} status: {status}")

        if status == "succeeded":
            print("Airbyte sync completed successfully")
            return True

        if status in ["failed", "cancelled"]:
            raise Exception(f"Airbyte sync failed. Job ID: {job_id}")

        time.sleep(poll_interval)
