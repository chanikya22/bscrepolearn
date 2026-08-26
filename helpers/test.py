import os
import sys
import boto3
from botocore.exceptions import ClientError

# Assuming your download method and create_s3_client are in a separate module
# You'll need to import them or include them in this script
# from your_module import download_files_from_s3_folder, create_s3_client, file_unzipper
from utils.s3utility import download_files_from_s3_folder

def get_s3_subdirectories(bucket_name, folder_path):
    """
    Get list of subdirectories in the S3 folder path
    """
    try:
        s3_client = boto3.client('s3')
        
        # Ensure folder_path ends with /
        if not folder_path.endswith('/'):
            folder_path += '/'
            
        # List objects with the folder prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=folder_path,
            Delimiter='/'
        )
        
        subdirectories = set()
        for page in page_iterator:
            # Get subdirectories from CommonPrefixes
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    # Extract directory name from prefix
                    subdir = prefix['Prefix'].replace(folder_path, '').rstrip('/')
                    if subdir:  # Skip empty strings
                        subdirectories.add(subdir)
        
        return list(subdirectories)
        
    except ClientError as e:
        print(f"Error accessing S3: {e}")
        return []

def get_local_directories(local_dir):
    """
    Get list of directories in the local folder
    """
    if not os.path.exists(local_dir):
        return []
    
    return [d for d in os.listdir(local_dir) 
            if os.path.isdir(os.path.join(local_dir, d))]

def download_missing_directories(bucket_name, folder_path, local_dir):
    """
    Download only directories that don't exist locally
    """
    print("Checking for existing directories...")
    
    # Get S3 subdirectories
    s3_subdirs = get_s3_subdirectories(bucket_name, folder_path)
    print(f"Found {len(s3_subdirs)} subdirectories in S3:")
    for subdir in sorted(s3_subdirs):
        print(f"  - {subdir}")
    
    # Get local directories
    local_subdirs = get_local_directories(local_dir)
    print(f"\nFound {len(local_subdirs)} local directories:")
    for subdir in sorted(local_subdirs):
        print(f"  - {subdir}")
    
    # Find directories to download
    missing_dirs = [d for d in s3_subdirs if d not in local_subdirs]
    
    if not missing_dirs:
        print("\nAll directories already exist locally. Nothing to download.")
        return
    
    print(f"\nDirectories to download: {len(missing_dirs)}")
    for subdir in sorted(missing_dirs):
        print(f"  - {subdir}")
    
    # Download each missing directory
    for subdir in missing_dirs:
        print(f"\n{'='*60}")
        print(f"Downloading directory: {subdir}")
        print(f"{'='*60}")
        
        # Construct full S3 path for this subdirectory
        s3_subdir_path = f"{folder_path.rstrip('/')}/{subdir}/"
        local_subdir_path = os.path.join(local_dir, subdir)
        
        try:
            # Create local subdirectory if it doesn't exist
            os.makedirs(local_subdir_path, exist_ok=True)
            
            # Download files from this S3 subdirectory
            download_files_from_s3_folder(bucket_name, s3_subdir_path, local_subdir_path)
            print(f"✓ Successfully downloaded: {subdir}")
            
        except Exception as e:
            print(f"✗ Error downloading {subdir}: {str(e)}")
            continue

def test_s3_download():
    """Test function to download the specified S3 folder with skip logic"""
    
    # S3 configuration
    bucket_name = "bsc-file-automation"
    folder_path = "vinculum/orderpull/raw/year=2025/month=06/"
    local_dir = r"C:\Users\shish\Downloads\alljsonfiles"
    
    print(f"Starting intelligent S3 download...")
    print(f"Bucket: {bucket_name}")
    print(f"Folder: {folder_path}")
    print(f"Local directory: {local_dir}")
    print("-" * 50)
    
    try:
        # Create local directory if it doesn't exist
        os.makedirs(local_dir, exist_ok=True)
        
        # Download only missing directories
        download_missing_directories(bucket_name, folder_path, local_dir)
        
        print("-" * 50)
        print("Download process completed!")
        
    except Exception as e:
        print(f"Error during download: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    test_s3_download()