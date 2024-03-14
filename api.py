import json
# https://stackoverflow.com/questions/53176162/google-oauth-scope-changed-during-authentication-but-scope-is-same
import os
from datetime import datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
from pathlib import Path

os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '1'

GOOGLE_TOKEN_PATH = Path(__file__).parent.resolve() / 'configs' / 'gapi_creds.json'
GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.settings.basic',
    'https://www.googleapis.com/auth/drive',
]
GOOGLE_SECRET_PATH = Path(__file__).parent.resolve() / 'configs' / 'google_secret.json'

def auth():
    creds = None
    if GOOGLE_TOKEN_PATH.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(GOOGLE_TOKEN_PATH), GOOGLE_SCOPES)
        except:
            pass
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            assert GOOGLE_SECRET_PATH.exists(), "Use should use your own credentials in kwargs " \
                                                f"or create {GOOGLE_SECRET_PATH}. " \
                                                f"How to get it: " \
                                                f"https://pythonhosted.org/PyDrive/quickstart.html#authentication"
            google_secret = json.loads(GOOGLE_SECRET_PATH.read_bytes())
            flow = InstalledAppFlow.from_client_config(google_secret, GOOGLE_SCOPES)
            try:
                creds = flow.run_local_server()
            except OSError:
                creds = flow.authorization_url()
        with open(GOOGLE_TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    json_creds = json.loads(creds.to_json())

    try:
        expire_datetime = datetime.strptime(json_creds['expiry'], "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        expire_datetime = datetime.strptime(json_creds['expiry'], "%Y-%m-%dT%H:%M:%SZ")
    oauth_creds = GoogleCredentials(
            access_token=json_creds['token'],
            client_id=json_creds['client_id'],
            client_secret=json_creds['client_secret'],
            refresh_token=json_creds['refresh_token'],
            token_expiry=expire_datetime,
            token_uri=json_creds['token_uri'],
            user_agent='Python client library')

    return oauth_creds


def get_service(service_name, creds=None):
    if creds is None:
        creds = auth()
    if service_name == 'slides':
        return build(service_name, 'v1', credentials=creds).presentations()
    elif service_name == 'sheets':
        return build(service_name, 'v4', credentials=creds).spreadsheets()
    elif service_name == 'drive':
        return build(service_name, 'v3', credentials=creds)
    else:
        raise NotImplementedError
