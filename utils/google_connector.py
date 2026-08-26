import gspread
from google.oauth2.service_account import Credentials
import os
def connect_to_google_sheet():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, '..', 'utils', 'google_credentials', 'starlit-tangent-411613-a5467af2ab19.json')
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client

