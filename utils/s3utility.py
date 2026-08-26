import json
import shutil
import zipfile
from typing import List, Dict, Any, Union, Optional
import pandas as pd
import boto3
import os
import io
import awswrangler as wr
from dotenv import load_dotenv
import concurrent.futures
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
import environmentconfig


def create_s3_client():
    load_dotenv()  # Load environment variables from .env file
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name="ap-south-1",
        config=Config(signature_version="s3v4")
    )
    return s3


def create_s3_session():
    """Create and return a boto3 session using credentials from environment variables"""
    load_dotenv()
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_REGION', 'ap-south-1')

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )
    return session


def read_s3_parquet(path):
    """
    Read parquet file(s) from S3 into a pandas DataFrame.

    Args:
        path: S3 path in format 's3://bucket-name/path/to/file.parquet'
             Can also be a prefix for multiple files

    Returns:
        pandas.DataFrame: Data from the parquet file(s)
    """
    session = create_s3_session()
    return wr.s3.read_parquet(path=path, boto3_session=session)


def write_s3_parquet(df, path, partition_cols=None, dedup_cols=None, file_name="data.parquet", overwrite=False):
    """
    Write DataFrame to S3 as parquet file(s), with optional partitioning and deduplication.

    Args:
        df: pandas.DataFrame to write
        path: S3 path in format 's3://bucket-name/path/to/file' or 's3://bucket-name/path/to/file.parquet'
        partition_cols: List of column names to partition by (optional)
        dedup_cols: List of column names to use for deduplication (optional)
        file_name: Name of file to use when partitioning (only used if dedup_cols is provided)
        overwrite: If True, delete existing file(s) at the path before writing (default: False)

    Returns:
        List of written paths
    """

    # Handle overwrite - delete existing files if requested
    if overwrite:
        session = create_s3_session()
        try:
            # Check if path exists and delete
            if wr.s3.does_object_exist(path=path, boto3_session=session):
                print(f"Deleting existing file(s) at: {path}")
                wr.s3.delete_objects(path=path, boto3_session=session)
        except Exception as e:
            print(f"Note: Could not check/delete existing path: {e}")

    # If no deduplication needed, use awswrangler directly
    if dedup_cols is None:
        session = create_s3_session()
        result = wr.s3.to_parquet(
            df=df,
            path=path,
            boto3_session=session,
            partition_cols=partition_cols,
            dataset=True if partition_cols else False,
            index=False
        )

        # Extract just the path(s)
        if 'paths' in result:
            paths = result['paths']
            # Return single string if only one path, otherwise return the list
            return paths[0] if len(paths) == 1 else paths
        return path

    # Extract bucket and path from s3 path
    if not path.startswith('s3://'):
        raise ValueError("Path must start with 's3://'")

    parts = path[5:].split('/', 1)
    if len(parts) < 2:
        raise ValueError("Path must include bucket name and path: 's3://bucket-name/path'")

    bucket_name = parts[0]
    base_path = parts[1]

    # Remove .parquet extension from base_path if it exists
    if base_path.endswith('.parquet'):
        base_path = base_path[:-8]

    # Use save_partitioned_data for deduplication
    print(f"Using save_partitioned_data for partitioning and deduplication")
    return save_partitioned_data(
        df=df,
        base_path=base_path,
        partition_cols=partition_cols,
        file_name=file_name,
        bucket_name=bucket_name,
        dedup_cols=dedup_cols
    )


def list_s3_files(bucket, prefix="", suffix=""):
    """
    List files in an S3 bucket with optional prefix and suffix filtering.

    Args:
        bucket: S3 bucket name
        prefix: Prefix to filter objects (optional)
        suffix: Suffix to filter objects (optional)

    Returns:
        List of S3 keys
    """
    s3 = create_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = []

    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if suffix and not key.endswith(suffix):
                continue
            files.append(key)

    return files

def upload_file_to_s3(file_path, bucket_name, s3_key):
    """
    Uploads a file (local or in-memory) to S3 using the created S3 client.

    :param file_path: Path to the file to upload (can be local or an in-memory object).
    :param bucket_name: Name of the S3 bucket.
    :param s3_key: S3 object key (path within the bucket, including the file name).
    """
    # Create the S3 client
    s3_client = create_s3_client()

    try:
        # Check if file_path is a local file or in-memory object
        if isinstance(file_path, str) and os.path.isfile(file_path):
            # Upload local file
            with open(file_path, 'rb') as file:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=file,
                    ContentType='application/octet-stream'  # Can be customized based on file type
                )
            print(f"File uploaded successfully to s3://{bucket_name}/{s3_key}")
        else:
            # Assume file_path is in-memory (bytes-like object)
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=file_path,  # file_path here is treated as in-memory content
                ContentType='application/octet-stream'  # Can be customized based on file type
            )
            print(f"In-memory file uploaded successfully to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload file to S3: {e}")


def download_file_to_buffer(bucket_name, s3_key):
    """
    Downloads a file from S3 into an in-memory buffer.

    :param bucket_name: Name of the S3 bucket.
    :param s3_key: Key (path) of the file in the S3 bucket.
    :return: A buffer containing the file's contents.
    """
    # Create an S3 client
    s3 = create_s3_client()

    # Create an in-memory buffer
    buffer = io.BytesIO()

    try:
        # Download the file to the buffer
        s3.download_fileobj(Bucket=bucket_name, Key=s3_key, Fileobj=buffer)
        buffer.seek(0)  # Reset buffer's pointer to the beginning
        print(f"Successfully downloaded {s3_key} from S3 to buffer.")
        return buffer
    except Exception as e:
        print(f"Error downloading {s3_key} to buffer: {e}")
        raise

#folder_path is the path of the folder in the S3 bucket from which files need to be downloaded
#local_dir is the local directory where the files will be downloaded
# Example usage:
# download_files_from_s3_folder('my-bucket', 'my-folder/', 'local-directory/')


def upload_file_with_expiry(
    file: bytes | str,
    bucket_name: str,
    s3_key: str,
    content_type: str = "application/octet-stream",
    expiry_days: int = 7,
) -> str | None:
    """
    Upload a file to S3 and return a pre-signed URL with configurable expiry.

    Args:
        file:          Raw bytes OR a local file path string
        bucket_name:   S3 bucket name
        s3_key:        Full S3 key (path + filename)
        content_type:  MIME type (e.g. "image/png", "text/csv")
        expiry_days:   Pre-signed URL validity in days (default: 7)

    Returns:
        Pre-signed URL string, or None if upload failed
    """
    try:
        s3 = create_s3_client()

        if isinstance(file, str):
            with open(file, "rb") as f:
                body = f.read()
        else:
            body = file

        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=body,
            ContentType=content_type,
        )

        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": s3_key},
            ExpiresIn=expiry_days * 24 * 3600,
        )
        return url

    except Exception as e:
        print(f"Failed to upload file to S3 with expiry: {e}")
        return None



def save_df_to_s3_parquet(
        df: pd.DataFrame,
        s3_path: str,
        partition_cols: Optional[Union[List[str], str]] = None,
        dedup_cols: Optional[List[str]] = None,
        file_name: str = "data.parquet",
        compression: str = 'snappy',
        use_awswrangler: bool = True
) -> Union[str, List[str]]:
    """
    Unified function to save DataFrames to S3 as Parquet files with or without partitioning.

    This function handles both partitioned and non-partitioned cases, with optional deduplication.
    It can use either awswrangler (preferred for most cases) or a custom implementation for
    special requirements like deduplication.

    Args:
        df: pandas DataFrame to write
        s3_path: S3 path in format 's3://bucket-name/path/to/file'
        partition_cols: Column(s) to partition by (string or list of strings), or None for no partitioning
        dedup_cols: List of columns to use for deduplication against existing data (None for no deduplication)
        file_name: Name of file to save (only used for partitioned data or when using custom implementation)
        compression: Compression algorithm ('snappy', 'gzip', etc.)
        use_awswrangler: Whether to use awswrangler library (more efficient for standard cases)

    Returns:
        String or list of strings representing the S3 path(s) where data was written
    """
    print(f"Saving DataFrame with {len(df)} rows to {s3_path}")

    # Always validate the s3_path format
    if not s3_path.startswith('s3://'):
        raise ValueError("Path must start with 's3://'")

    # Extract bucket and path
    parts = s3_path[5:].split('/', 1)
    if len(parts) < 2:
        raise ValueError("Path must include bucket name and path: 's3://bucket-name/path'")

    bucket_name = parts[0]
    base_path = parts[1]

    # Remove .parquet extension if it exists
    if base_path.endswith('.parquet'):
        base_path = base_path[:-8]

    # Case 1: Not partitioned, not using deduplication - simple single file case
    if not partition_cols and not dedup_cols:
        if use_awswrangler:
            try:
                import awswrangler as wr
                session = create_s3_session()
                result = wr.s3.to_parquet(
                    df=df,
                    path=s3_path,
                    boto3_session=session,
                    compression=compression,
                    index=False
                )
                return result if isinstance(result, str) else result.get('paths', [s3_path])[0]
            except ImportError:
                print("awswrangler not available, falling back to manual implementation")
                use_awswrangler = False

        if not use_awswrangler:
            # Manual implementation for single file case
            try:
                # Ensure the path ends with .parquet
                if not s3_path.endswith('.parquet'):
                    s3_path = f"{s3_path}.parquet"

                # Write DataFrame to in-memory buffer
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False, compression=compression)
                buffer.seek(0)

                # Extract key from full path
                s3_key = s3_path[5 + len(bucket_name) + 1:]  # Skip 's3://' + bucket_name + '/'

                # Upload to S3
                upload_file_to_s3(buffer, bucket_name, s3_key)
                return s3_path
            except Exception as e:
                print(f"Error saving non-partitioned data to S3: {e}")
                raise

    # Case 2: Using awswrangler for partitioned data without deduplication
    if partition_cols and not dedup_cols and use_awswrangler:
        try:
            import awswrangler as wr
            session = create_s3_session()

            # Convert string partition_cols to list if needed
            if isinstance(partition_cols, str):
                partition_cols = [partition_cols]

            result = wr.s3.to_parquet(
                df=df,
                path=s3_path,
                boto3_session=session,
                partition_cols=partition_cols,
                compression=compression,
                dataset=True,
                index=False
            )

            # Extract paths from result
            if isinstance(result, dict) and 'paths' in result:
                return result['paths']
            return s3_path

        except ImportError:
            print("awswrangler not available, falling back to manual implementation")
            use_awswrangler = False

    # Case 3: Using custom implementation for partitioned data with or without deduplication
    if partition_cols or dedup_cols:
        # Convert string partition_cols to list if needed
        if isinstance(partition_cols, str):
            partition_cols = [partition_cols]
        elif partition_cols is None:
            # If no partition columns, we'll partition by an artificial column to use the same code path
            df['_temp_no_partition'] = 1
            partition_cols = ['_temp_no_partition']

        # Check partition columns exist in DataFrame
        missing_cols = [col for col in partition_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Partition columns {missing_cols} not found in DataFrame")

        # Save partitioned data
        s3_paths = []

        # Group by partition columns and process each partition
        for partition_values, partition_df in df.groupby(partition_cols):
            # Handle single partition column case
            if not isinstance(partition_values, tuple):
                partition_values = (partition_values,)

            # Build partition path
            partition_path = f"{base_path}/" + "/".join(
                [f"{col}={val}" for col, val in zip(partition_cols, partition_values)
                 if col != '_temp_no_partition']  # Skip artificial column
            )

            # If using the artificial column, don't add it to the path
            if '_temp_no_partition' in partition_cols and len(partition_cols) == 1:
                partition_path = base_path

            s3_key = f"{partition_path}/{file_name}"

            # Attempt to load existing data if deduplication is requested
            if dedup_cols:
                try:
                    print(f"Checking for existing file at s3://{bucket_name}/{s3_key}")
                    buffer = download_file_to_buffer(bucket_name, s3_key)
                    buffer.seek(0)
                    existing_df = pd.read_parquet(buffer)

                    # Merge and deduplicate
                    print(f"Deduplicating against existing data using columns: {dedup_cols}")
                    merged_df = pd.concat([existing_df, partition_df]).drop_duplicates(
                        subset=dedup_cols, keep="last"
                    )
                    print(f"Merged data: {len(merged_df)} rows after deduplication")
                except Exception as e:
                    print(f"No existing data found for {s3_key}. Creating new file. Error: {e}")
                    merged_df = partition_df
            else:
                merged_df = partition_df

            # Drop the artificial column if it was added
            if '_temp_no_partition' in merged_df.columns and '_temp_no_partition' in partition_cols:
                merged_df = merged_df.drop(columns=['_temp_no_partition'])

            # Write to buffer and upload
            output_buffer = io.BytesIO()
            merged_df.to_parquet(output_buffer, index=False, compression=compression)
            output_buffer.seek(0)

            # Upload to S3
            upload_file_to_s3(output_buffer, bucket_name, s3_key)
            s3_paths.append(f"s3://{bucket_name}/{s3_key}")
            print(f"Saved partition to S3: {s3_key} with {len(merged_df)} records")

        return s3_paths[0] if len(s3_paths) == 1 else s3_paths

    # Should never reach here, but just in case
    raise ValueError("Invalid combination of parameters")


def download_s3_folder_efficiently(bucket_name, s3_folder, local_dir, max_workers=20):
    """
    Download files from an S3 folder efficiently using parallel processing.

    Args:
        bucket_name: Name of the S3 bucket
        s3_folder: Path to the folder in S3
        local_dir: Local directory to save files to
        max_workers: Number of parallel downloads (20 is a good balance)
    """
    # Create S3 client with appropriate credentials
    s3 = create_s3_client()

    # Configure transfer with multipart for larger files
    config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,  # 8MB
        max_concurrency=10,
        use_threads=True
    )

    # Ensure the local directory exists
    os.makedirs(local_dir, exist_ok=True)

    # Get all file keys (may take a moment for 5k-6k files)
    print(f"Listing all files in s3://{bucket_name}/{s3_folder}...")
    paginator = s3.get_paginator('list_objects_v2')

    file_keys = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_folder):
        if 'Contents' in page:
            file_keys.extend([
                obj['Key'] for obj in page['Contents']
                if not obj['Key'].endswith('/')
            ])

    total_files = len(file_keys)
    print(f"Found {total_files} files to download")

    # Download function for each worker
    def download_file(key):
        try:
            # Create relative path
            rel_path = os.path.relpath(key, s3_folder)
            local_file_path = os.path.join(local_dir, rel_path)

            # Create directory structure if needed
            local_file_dir = os.path.dirname(local_file_path)
            if not os.path.exists(local_file_dir):
                os.makedirs(local_file_dir, exist_ok=True)

            # Download with transfer config
            s3.download_file(
                bucket_name,
                key,
                local_file_path,
                Config=config
            )
            return key
        except Exception as e:
            return f"Error downloading {key}: {str(e)}"

    # Use progress tracking
    completed = 0
    errors = []

    # Download files in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_key = {executor.submit(download_file, key): key for key in file_keys}

        # Process as they complete
        for future in concurrent.futures.as_completed(future_to_key):
            result = future.result()
            completed += 1

            # Simple progress indicator
            if isinstance(result, str) and result.startswith("Error"):
                errors.append(result)
                print(f"Progress: {completed}/{total_files} - ERROR")
            else:
                if completed % 100 == 0 or completed == total_files:
                    print(f"Progress: {completed}/{total_files} ({completed / total_files * 100:.1f}%)")

    # Final summary
    print(f"Download complete: {completed - len(errors)}/{total_files} files successful")
    if errors:
        print(f"{len(errors)} errors occurred")
        for error in errors[:10]:  # Show first 10 errors
            print(f"  {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")

    return completed - len(errors)


def download_files_from_s3_folder(bucket_name, folder_path, local_dir):
    s3 = create_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_path)

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('/'):
                    continue
                local_file_path = os.path.join(local_dir, os.path.relpath(key, folder_path))
                local_file_dir = os.path.dirname(local_file_path)
                if not os.path.exists(local_file_dir):
                    os.makedirs(local_file_dir)
                s3.download_file(bucket_name, key, local_file_path)
                print(f"Downloaded {key} to {local_file_path}")
                if local_file_path.lower().endswith('.zip'):
                   file_unzipper(local_file_path)

def file_unzipper(local_file_path):
    local_file_dir = os.path.dirname(local_file_path)

    print(f"Extracting zip file: {local_file_path}")

    # Copy the zip file to avoid file lock issues
    temp_zip_path = local_file_path + ".temp.zip"
    shutil.copy2(local_file_path, temp_zip_path)

    try:
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            # Get list of files before extraction
            zip_files = zip_ref.namelist()

            # Extract all files directly to the same folder as the zip
            zip_ref.extractall(local_file_dir)
            print(f"Successfully extracted {len(zip_files)} files from {local_file_path}")

        # Delete the original zip file
        os.remove(local_file_path)
        print(f"Deleted original zip file: {local_file_path}")
    except Exception as e:
        print(f"Error during extraction: {e}")
    finally:
        # Clean up the temporary zip file
        if os.path.exists(temp_zip_path):
            os.remove(temp_zip_path)
            print(f"Cleaned up temporary zip file")
# Example usage:
# delete_file_from_s3('your-bucket-name', 'your-file-key')
def delete_file_from_s3(bucket_name, file_key):
    s3 = create_s3_client()
    try:
        s3.delete_object(Bucket=bucket_name, Key=file_key)
        print(f"Deleted file: {file_key} from bucket: {bucket_name}")
    except Exception as e:
        print(f"Error deleting file {file_key} from bucket {bucket_name}: {e}")


def save_partitioned_data(df, base_path, partition_cols, file_name, bucket_name, dedup_cols=None):
    """
    Saves partitioned data to S3 as Parquet files, performing deduplication if needed.

    Args:
        df: DataFrame to save
        base_path: Base S3 path without bucket name
        partition_cols: List of columns to partition by
        file_name: Name of the file to save in each partition
        bucket_name: S3 bucket name
        dedup_cols: List of columns to use for deduplication (if None, no deduplication is performed)

    Returns:
        List of S3 paths where files were written
    """
    import io
    import pandas as pd
    import logging

    if not isinstance(partition_cols, list):
        partition_cols = [partition_cols]

    # Make sure partition columns exist in the DataFrame
    missing_cols = [col for col in partition_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Partition columns {missing_cols} not found in DataFrame")

    s3_paths = []

    # Group by partition columns
    for partition_values, partition_df in df.groupby(partition_cols):
        # Handle single partition column case
        if not isinstance(partition_values, tuple):
            partition_values = (partition_values,)

        # Build partition path
        partition_path = f"{base_path}/" + "/".join(
            [f"{col}={val}" for col, val in zip(partition_cols, partition_values)]
        )
        s3_key = f"{partition_path}/{file_name}"

        # Attempt to load existing data if deduplication is requested
        if dedup_cols:
            try:
                print(f"Checking for existing file at s3://{bucket_name}/{s3_key}")
                buffer = download_file_to_buffer(bucket_name, s3_key)
                buffer.seek(0)
                existing_df = pd.read_parquet(buffer)

                # Merge and deduplicate
                print(f"Deduplicating against existing data using columns: {dedup_cols}")
                merged_df = pd.concat([existing_df, partition_df]).drop_duplicates(subset=dedup_cols, keep="last")
                print(f"Merged data: {len(merged_df)} rows after deduplication")
            except Exception as e:
                print(f"No existing data found for {s3_key}. Creating new file. Error: {e}")
                merged_df = partition_df
        else:
            merged_df = partition_df

        # Write to buffer and upload
        output_buffer = io.BytesIO()
        merged_df.to_parquet(output_buffer, index=False)
        output_buffer.seek(0)

        # Upload to S3
        upload_file_to_s3(output_buffer, bucket_name, s3_key)
        s3_paths.append(f"s3://{bucket_name}/{s3_key}")
        print(f"Saved partition to S3: {s3_key} with {len(merged_df)} records")

    return s3_paths


def get_secrets(secret_name):
    try:
        # 1. Retrieve credentials from config file
        load_dotenv()  # Load environment variables from .env file
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        # 2. Create Secrets Manager client
        session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name="ap-south-1"
        )
        client = session.client(service_name='secretsmanager')

        # 3. Fetch the secret
        response = client.get_secret_value(
            SecretId=secret_name,
            VersionStage='AWSCURRENT'  # or any specific version stage
        )

        # 4. Parse the JSON string into a dictionary
        secret_json = response['SecretString']
        secrets_dictionary = json.loads(secret_json)

        return secrets_dictionary

    except ClientError as e:
        # Handle specific AWS client errors
        raise Exception(f"Error retrieving secrets from AWS Secrets Manager: {str(e)}")
    except Exception as e:
        # Handle other exceptions
        raise Exception(f"Error retrieving secrets from AWS Secrets Manager: {str(e)}")
