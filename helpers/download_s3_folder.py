import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pathlib import Path

def download_s3_folder(bucket_name, s3_folder_path, local_folder_path):
    """
    Download all contents of an S3 folder to a local directory
    
    Args:
        bucket_name (str): Name of the S3 bucket
        s3_folder_path (str): S3 folder path (e.g., 'data/files/')
        local_folder_path (str): Local directory path to save files
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Ensure S3 folder path ends with /
        if not s3_folder_path.endswith('/'):
            s3_folder_path += '/'
        
        # Create local directory if it doesn't exist
        Path(local_folder_path).mkdir(parents=True, exist_ok=True)
        
        print(f"Downloading from S3://{bucket_name}/{s3_folder_path}")
        print(f"Saving to: {local_folder_path}")
        print("-" * 60)
        
        # List all objects in the S3 folder
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_folder_path)
        
        download_count = 0
        total_size = 0
        
        for page in pages:
            if 'Contents' not in page:
                print("No files found in the specified S3 folder.")
                return
            
            for obj in page['Contents']:
                s3_key = obj['Key']
                file_size = obj['Size']
                
                # Skip if it's just the folder itself (ends with /)
                if s3_key.endswith('/'):
                    continue
                
                # Create relative path by removing the S3 folder prefix
                relative_path = s3_key[len(s3_folder_path):]
                local_file_path = os.path.join(local_folder_path, relative_path)
                
                # Create local subdirectories if needed
                local_file_dir = os.path.dirname(local_file_path)
                if local_file_dir:
                    Path(local_file_dir).mkdir(parents=True, exist_ok=True)
                
                try:
                    # Download the file
                    print(f"Downloading: {relative_path} ({file_size:,} bytes)")
                    s3_client.download_file(bucket_name, s3_key, local_file_path)
                    download_count += 1
                    total_size += file_size
                    
                except Exception as file_error:
                    print(f"Error downloading {relative_path}: {file_error}")
                    continue
        
        print("-" * 60)
        print(f"Download completed!")
        print(f"Files downloaded: {download_count}")
        print(f"Total size: {total_size:,} bytes ({total_size / (1024*1024):.2f} MB)")
        
    except NoCredentialsError:
        print("Error: AWS credentials not found. Please configure your AWS credentials.")
        print("You can use 'aws configure' or set environment variables.")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            print(f"Error: Bucket '{bucket_name}' does not exist.")
        elif error_code == 'AccessDenied':
            print(f"Error: Access denied to bucket '{bucket_name}' or folder '{s3_folder_path}'.")
        else:
            print(f"AWS Error: {e}")
            
    except Exception as e:
        print(f"Unexpected error: {e}")

def main():
    """
    Main function - Configure your S3 details here
    """
    # Configuration - Update these values
    BUCKET_NAME = "bsc-file-automation"
    S3_FOLDER_PATH = "vinculum/invoicedetail/raw/year=2025/month=06/14f03d0af07ec5c571e198e86087b62bc83c5db8/"  # e.g., "data/reports/2024/"
    LOCAL_FOLDER_PATH = r"C:\Users\shish\Downloads\alljsonfiles1"  # or "/home/user/downloads/s3_download"
    
    print("S3 Folder Download Script")
    print("=" * 40)
    
    
    
    # Start download
    download_s3_folder(BUCKET_NAME, S3_FOLDER_PATH, LOCAL_FOLDER_PATH)

if __name__ == "__main__":
    main()