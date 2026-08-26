import time
import os
import tempfile
import json
import requests

from utils.s3utility import create_s3_client

def join_paths(paths, separator=","):
    """
    Join a list of paths with the specified separator.
    Works with both flat lists of strings and nested lists.
    Removes duplicate paths while preserving the original order.

    Args:
        paths: A list of paths (strings) or nested lists of paths
        separator: The separator to use (default: comma)

    Returns:
        A string with all unique paths joined by the separator
    """
    result = []
    seen = set()

    def flatten(items):
        for item in items:
            if isinstance(item, list):
                flatten(item)
            elif isinstance(item, str):
                if item not in seen:
                    seen.add(item)
                    result.append(item)
            else:
                # Convert non-string items to strings
                item_str = str(item)
                if item_str not in seen:
                    seen.add(item_str)
                    result.append(item_str)

    flatten(paths)
    return separator.join(result)

def load_parquet_to_iceberg(processed_data_paths, bucket_name, target_table, target_schema, key_columns, postgres_connector, business_key_columns=None):
    """
    Load processed parquet files into Iceberg tables

    Args:
        processed_data_paths: List of S3 paths to parquet files
        bucket_name: S3 bucket name
        target_table: Target Iceberg table name
        target_schema: Target schema name
        key_columns: Primary key columns for upserting
        postgres_connector: Instance of PostgresConnector
        business_key_columns: Business key columns (optional, defaults to None)

    Returns:
        Dictionary with summary of operation results
    """
    results = {
        'total_files': len(processed_data_paths),
        'successful': 0,
        'failed': 0,
        'records_processed': 0,
        'failed_files': []
    }

    # Process each parquet file
    for path in processed_data_paths:
        print(f"Loading data from {path} to {target_table}")

        try:
            # Generate a unique staging table name
            staging_table = f"{target_table}_staging_{int(time.time())}"

            # Load data from S3 to Iceberg table
            load_result = postgres_connector.load_from_s3_to_iceberg(
                s3_path=path,
                target_table=target_table,
                key_columns=key_columns,
                staging_table=staging_table,
                business_key_columns=business_key_columns,
                schema=target_schema
            )

            if load_result['success']:
                results['successful'] += 1
                records = load_result.get('inserted_count', 0)
                results['records_processed'] += records
                print(f"Successfully loaded {records} records from {path}")
            else:
                results['failed'] += 1
                results['failed_files'].append({
                    'path': path,
                    'error': load_result.get('error', 'Unknown error')
                })
                print(f"Failed to load {path}: {load_result.get('error', 'Unknown error')}")

        except Exception as e:
            results['failed'] += 1
            results['failed_files'].append({
                'path': path,
                'error': str(e)
            })
            print(f"Error processing {path}: {str(e)}")

    print(f"Summary: {results['successful']} files loaded successfully, {results['failed']} failed")
    print(f"Total records processed: {results['records_processed']}")

    return results


def download_insyt_token_from_s3():
    s3_client = create_s3_client()

    temp_dir = tempfile.mkdtemp()

    # Create directory if it doesn't exist
    os.makedirs(temp_dir, exist_ok=True)

    # Define the local file path
    token_temp_file = os.path.join(temp_dir, 'token.json')

    # Make sure locations_csv_s3_path is correctly defined as just the object key
    # For example: "data/locations.csv" (not "s3://bucket-name/data/locations.csv")
    s3_client.download_file('bsc-file-automation', 'token.json', token_temp_file)

    with open(os.path.join(temp_dir, 'token.json'), 'r', encoding='utf-8', errors='ignore') as f:
        data = json.load(f)
        token = data.get("token")
    return token


def insyt_file_uploader(file_path, api_url):
    """
    Upload a file to the Insyt API and handle response validation.

    Args:
        file_path: Path to the file to upload
        api_url: API endpoint URL

    Returns:
        Response data from API

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If upload fails or API returns error
    """
    # Check if file exists first
    if not os.path.isfile(file_path):
        error_msg = f"File not found: {file_path}"
        print(error_msg)
        raise FileNotFoundError(error_msg)

    # Get authentication token
    token = download_insyt_token_from_s3()
    headers = {
        'accept': '*/*',
        'Authorization': f'Bearer {token}'
    }

    try:
        # Open and upload the file
        with open(file_path, 'rb') as file:
            files = {
                'file': (os.path.basename(file_path), file, 'text/csv')
            }
            print(f"Uploading file: {os.path.basename(file_path)}")
            response = requests.post(api_url, headers=headers, files=files)

        # Parse response
        response_data = None
        try:
            response_data = response.json()
        except requests.exceptions.JSONDecodeError:
            # If response is not JSON, use text
            response_data = response.text

        # Check for success
        is_success = False

        # Check status code
        if response.status_code == 200:
            # Check response body for success indicators
            if isinstance(response_data, dict):
                # Check common success fields (case-insensitive)
                is_success_field = response_data.get('isSuccess') or \
                                   response_data.get('issuccess') or \
                                   response_data.get('success') or \
                                   response_data.get('Success')

                # If success field exists and is False, it's a failure
                if is_success_field is not None and not is_success_field:
                    error_message = response_data.get('message') or \
                                    response_data.get('error') or \
                                    response_data.get('Message') or \
                                    "API returned success=false"
                    print(f"Upload failed: {error_message}")
                    print(f"Response: {response_data}")
                    raise ValueError(f"API Error: {error_message}")
                else:
                    is_success = True
            else:
                # If response is not a dict, assume success based on status code
                is_success = True

        if is_success:
            print(f"✓ File uploaded successfully: {os.path.basename(file_path)}")
            print(f"Response: {response_data}")
            return True
        else:
            # Non-200 status code
            error_msg = f"Upload failed with status {response.status_code}"
            print(error_msg)
            print(f"Response: {response_data}")
            raise ValueError(f"{error_msg}. Response: {response_data}")

    except FileNotFoundError:
        raise
    except ValueError:
        raise
    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed: {str(e)}"
        print(error_msg)
        raise ValueError(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error during upload: {str(e)}"
        print(error_msg)
        raise ValueError(error_msg)
