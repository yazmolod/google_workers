import json
# https://stackoverflow.com/questions/53176162/google-oauth-scope-changed-during-authentication-but-scope-is-same
import os
from datetime import datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from oauth2client.client import GoogleCredentials
from pathlib import Path

os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '1'

GOOGLE_TOKEN_PATH = Path(__file__).parent.resolve() / 'configs' / 'gapi_creds.json'
GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/gmail.settings.basic',
    'https://www.googleapis.com/auth/drive',
]
GOOGLE_SECRET = json.loads((Path(__file__).parent.resolve() / 'configs' / 'google_secret.json').read_bytes())

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
            flow = InstalledAppFlow.from_client_config(GOOGLE_SECRET, GOOGLE_SCOPES)
            # creds = flow.run_console()
            creds = flow.run_local_server()
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