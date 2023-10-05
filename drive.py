import httplib2
from googleapiclient.discovery import build
from pydrive.drive import GoogleDrive
from google_workers.api import auth, get_service
from googleapiclient.http import MediaFileUpload
from pathlib import Path
from typing import Union
import google_auth_httplib2

PAGE_SIZE = 500

class GoogleAuthCopycat:
    '''GoogleAuth из модуля pydrive имеет странный механизм сохранения токена,
    из-за чего он становится неуниверсальным для других оболочек для google-api.
    Однако, pydrive.GoogleDrive требует этот класс для инициализации. Чтобы решить эту проблему, реализован этот класс,
    который быстро настраивается при помощи oauth2client.client.GoogleCredentials (или google.oauth2.credentials.Credentials)
    и в дальнейшем имитирует его методы и свойства
'''

    def __init__(self, creds):
        self.credentials = creds
        self.service = build('drive', 'v2', credentials=self.credentials)
        self.http_timeout = None

    @property
    def access_token_expired(self):
        return False

    def Get_Http_Object(self):
        http = httplib2.Http(timeout=self.http_timeout)
        http = self.credentials.authorize(http)
        return http


class GoogleDriveWorker:
    def __init__(self):
        self.creds = auth()
        self.gauth = GoogleAuthCopycat(self.creds)
        self.drive = GoogleDrive(self.gauth)
        self.API = get_service('drive')

    def thread_safety_execute(self, q):
        # для поддержки мультипоточности
        # https://googleapis.github.io/google-api-python-client/docs/thread_safety.html
        http = httplib2.Http()
        http = self.creds.authorize(http)
        r = q.execute(http=http)
        return r

    def iter_folders_in_folder(self, folder_id: str):
        pageToken = None
        q = "'{}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'".format(folder_id)
        while True:
            result = self.API.files().list(
                q=q,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageSize=PAGE_SIZE,
                pageToken=pageToken,
            ).execute()
            for i in result['files']:
                yield i
            pageToken = result.get('nextPageToken')
            if not pageToken:
                break

    def iter_files_in_folder(self, folder_id: str, deep: str = False, q: str = None):
        pageToken = None
        # https://developers.google.com/drive/api/guides/ref-search-terms
        full_query = "'{}' in parents and trashed=false and mimeType!='application/vnd.google-apps.folder'".format(
            folder_id)
        if q:
            full_query = full_query + " and " + q
        while True:
            result = self.API.files().list(
                q=full_query,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageSize=PAGE_SIZE,
                pageToken=pageToken,
            ).execute()
            for file in result['files']:
                yield file
            pageToken = result.get('nextPageToken')
            if not pageToken:
                break
        if deep:
            for folder in self.iter_folders_in_folder(folder_id=folder_id):
                yield from self.iter_files_in_folder(folder_id=folder["id"], deep=deep, q=q)

    def create_folder(self, folder_name, parent_folder_id):
        file_metadata = {
            'name': folder_name,
            'parents': [parent_folder_id],
            'mimeType': 'application/vnd.google-apps.folder'
        }
        folder = API.files().create(body=file_metadata, fields='id', supportsAllDrives=True).execute()
        return folder

    def search_file_by_name(self, folder_id: str, file_name: str):
        yield from self.iter_files_in_folder(folder_id=folder_id, q=f"name='{file_name}'")

    def move_file(self, file, to_folder_id: str):
        fileId = file['id']
        file_meta = self.get_file_meta(file, 'parents')
        if to_folder_id in file_meta['parents']:
            return
        prev_parents = ",".join(file_meta["parents"])
        file = self.thread_safety_execute(
            self.API.files().update(
                fileId=fileId,
                removeParents=prev_parents,
                addParents=to_folder_id,
                fields='id',
                supportsAllDrives=True)
        )
        return file

    def get_file_meta(self, file, fields):
        file_meta = self.thread_safety_execute(
            self.API.files().get(fileId=file['id'], fields=fields, supportsAllDrives=True))
        return file_meta

    def get_file_bytes(self, file):
        return self.API.files().get_media(fileId=file['id'], supportsAllDrives=True).execute()

    def upload_file(self, filepath: Union[str, Path], folder_id: str):
        filepath = Path(filepath)
        file_metadata = {
            'name': filepath.name,
            'parents': [folder_id],
        }
        media = MediaFileUpload(filepath)
        file = self.API.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
        return file

    def download_file(self, file, folder: Union[str, Path]):
        with open(Path(folder) / file['name'], 'wb') as f:
            f.write(self.get_file_bytes(file))

    def delete_file(self, file):
        file = self.API.files().delete(fileId=file['id'], supportsAllDrives=True).execute()
        return file