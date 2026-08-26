import os
import json
import yaml
import boto3
from botocore.exceptions import ClientError
import logging 

# Default to development environment
ENV = os.getenv("AIRFLOW_ENV", "dev")

# Check if running in Docker
IN_DOCKER = os.path.exists("/.dockerenv") or os.environ.get("RUNNING_IN_DOCKER") == "true"

# Allow developers to set their own base directory
if IN_DOCKER:
    # When in Docker, use container paths
    DEV_BASE_PATH = "/opt/airflow"
else:
    # For local development outside Docker
    # Chan Local Path
    DEV_BASE_PATH = os.getenv("DEV_BASE_PATH", "C:\\Users\\ChanikyaLadi\\Documents\\GitHub\\insytflow")
    # DEV_BASE_PATH = os.getenv("DEV_BASE_PATH", "D:/insytflow/latest-updated/insytflow/")

    # Chan Local Path
    DEV_BASE_PATH = os.getenv("DEV_BASE_PATH", "C:\\Users\\ChanikyaLadi\\Documents\\GitHub\\insytflow")

# Configuration for different environments
BASE_PATHS = {
    "dev": DEV_BASE_PATH,
    "prod": "/opt/airflow"
}

# Relative paths that remain consistent across environments
RELATIVE_PATHS = {
    "bsc_credentials_path": "connections/bsc/credentials.json",
    "neurogum_credentials_path": "connections/_100days/neurogum/credentials.json",
    "avon_credentials_path": "connections/_100days/avon/credentials.json",
    "google_neurogum_credentials_path": "connections/_100days/neurogum/google_ads/credentials.json",
    "google_credentials_path": "connections/bsc/shopify/google_ads/google_bsc/credentials.json",
    "google_bae_credentials_path": "connections/bsc/shopify/google_ads/google_bae/credentials.json",
    "date_neurogum_path": "connections/_100days/neurogum/google_ads/date.txt",
    "date_path": "connections/bsc/shopify/google_ads/google_bsc/date.txt",
    "date_bae_path": "connections/bsc/shopify/google_ads/google_bae/date.txt",
    "enamor_credentials_path": "connections/_100days/enamor/credentials.json",
    # Add other relative paths here
}

# ── ADD THIS BLOCK ──────────────────────────────────────────
def load_secrets_to_env(secret_name="insyt-flow-env", region_name="ap-south-1"):
    """Fetch all secrets from AWS and load into os.environ"""
    try:
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        response = client.get_secret_value(SecretId=secret_name)
        secrets = json.loads(response['SecretString'])

        for key, value in secrets.items():
            os.environ[key] = str(value)

        # ── MAP SMTP secrets to Airflow SMTP env vars ──
        os.environ['AIRFLOW__SMTP__SMTP_HOST']      = secrets.get('SMTP_HOST', '')
        os.environ['AIRFLOW__SMTP__SMTP_PORT']      = secrets.get('SMTP_PORT', '587')
        os.environ['AIRFLOW__SMTP__SMTP_USER']      = secrets.get('SMTP_USER', '')
        os.environ['AIRFLOW__SMTP__SMTP_PASSWORD']  = secrets.get('SMTP_PASSWORD', '')
        os.environ['AIRFLOW__SMTP__SMTP_MAIL_FROM'] = secrets.get('SMTP_USER', '')
        os.environ['AIRFLOW__SMTP__SMTP_STARTTLS']  = 'true'
        os.environ['AIRFLOW__SMTP__SMTP_SSL']       = 'false'
        # ───────────────────────────────────────────────

        logging.info("Secrets loaded into environment successfully")

    except ClientError as e:
        logging.error(f"Failed to load secrets: {e}")
        raise

# Call once at module level — runs automatically when environmentconfig is imported
load_secrets_to_env()


def get_config(key, load_json=False):
    """
    Get configuration value for the specified key.

    Args:
        key: Configuration key
        load_json: If True and the key points to a JSON file, load and return its contents

    Returns:
        File path, or parsed JSON if load_json=True
    """
    if key in RELATIVE_PATHS:
        base_path = BASE_PATHS.get(ENV)
        relative_path = RELATIVE_PATHS.get(key)
        path = os.path.join(base_path, relative_path)

        if load_json and path.endswith('.json'):
            try:
                with open(path, 'r') as file:
                    return json.load(file)
            except Exception as e:
                print(f"Error loading JSON from {path}: {str(e)}")
                return None
        return path
    return None

def load_yaml_config(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config