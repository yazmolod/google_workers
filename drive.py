import httplib2
from googleapiclient.discovery import build
from google_workers.api import auth, get_service
from googleapiclient.http import MediaFileUpload
from pathlib import Path
from typing import Union, Optional

PAGE_SIZE = 500


class GoogleDriveWorker:
    def __init__(self, credentials=None, support_all_drives=True):
        self.creds = credentials if credentials is not None else auth()
        self.support_all_drives = support_all_drives
        self.API = get_service('drive', self.creds)

    def _api_execute(self, method: str, **kwargs):
        method_func = getattr(self.API.files(), method)
        q = method_func(supportsAllDrives=self.support_all_drives, **kwargs)
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
            result = self._api_execute(
                'list',
                q=q,
                includeItemsFromAllDrives=self.support_all_drives,
                pageSize=PAGE_SIZE,
                pageToken=pageToken,
            )
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
            result = self._api_execute(
                "list",
                q=full_query,
                includeItemsFromAllDrives=self.support_all_drives,
                pageSize=PAGE_SIZE,
                pageToken=pageToken,
            )
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
        folder = self._api_execute("create", body=file_metadata, fields='id')
        return folder

    def search_file_by_name(self, folder_id: str, file_name: str):
        yield from self.iter_files_in_folder(folder_id=folder_id, q=f"name='{file_name}'")

    def move_file(self, file, to_folder_id: str):
        fileId = file['id']
        file_meta = self.get_file_meta(file, 'parents')
        if to_folder_id in file_meta['parents']:
            return
        prev_parents = ",".join(file_meta["parents"])
        file = self._api_execute(
            "update",
            fileId=fileId,
            removeParents=prev_parents,
            addParents=to_folder_id,
            fields='id',
        )
        return file

    def get_file_meta(self, file, fields):
        file_meta = self._api_execute(
            "get",
            fileId=file['id'],
            fields=fields,
        )
        return file_meta

    def get_file_bytes(self, file):
        return self._api_execute(
            "get_media",
            fileId=file['id'],
        )

    def upload_file(self, filepath: Union[str, Path], folder_id: str):
        filepath = Path(filepath)
        file_metadata = {
            'name': filepath.name,
            'parents': [folder_id],
        }
        media = MediaFileUpload(filepath)
        file = self._api_execute(
            "create",
            body=file_metadata,
            media_body=media,
            fields='id',
        )
        return file

    def download_file(self, file, folder: Union[str, Path], filename: Optional[str] = None) -> Path:
        filename = file.get("name") if filename is None else filename
        if filename is None:
            filename = self.get_file_meta(file, 'name')['name']
        folder = Path(folder)
        folder.mkdir(exist_ok=True, parents=True)
        new_file = folder / filename
        with open(new_file, 'wb') as f:
            f.write(self.get_file_bytes(file))
        return new_file

    def delete_file(self, file):
        '''If the file belongs to a shared drive, the user must be an organizer on the parent folder'''
        file = self._api_execute(
            "delete",
            fileId=file['id'],
        )
        return file

    def trash_file(self, file):
        file = self._api_execute(
            "update",
            fileId = file['id'],
            body = {
                "trashed": True,
            }
        )
        return file
    
    def untrash_file(self, file):
        file = self._api_execute(
            "update",
            fileId = file['id'],
            body = {
                "trashed": False,
            }
        )
        return file