import os

import requests
import boto3
import json
from utils.s3utility import upload_file_to_s3
from dotenv import load_dotenv, find_dotenv
import environmentconfig
def get_auth_token(tenant_id=1, email="ayushgoyal@bombayshavingcompany.com", password="ayu@123",is_sign_in_with_google=False):
    """
    Login to the authentication service.

    Args:
        tenant_id (int): The tenant ID for the organization
        email (str): User's email address
        password (str): User's password
        is_sign_in_with_google (bool): Whether signing in with Google

    Returns:
        dict: Response from the API containing authentication details
    """
    env_file = ".env"
    dotenv_path = find_dotenv(env_file) if env_file else find_dotenv()
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
    access_token = os.getenv("x-core-access-token")
    url = "http://3.7.121.98:6110/auth/login"

    headers = {
        "x-core-access-token": access_token,
        "Content-Type": "application/json"
    }

    payload = {
        "tenantId": tenant_id,
        "email": email,
        "password": password,
        "isSignInWithGoogle": is_sign_in_with_google
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        response_data = response.json()
        with open('token.json', 'w') as f:
            json.dump(response_data, f, indent=2)

        upload_file_to_s3(
            file_path='token.json',
            bucket_name='bsc-file-automation',
            s3_key='token.json'
        )
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


if __name__ == "__main__":
    print(get_auth_token())