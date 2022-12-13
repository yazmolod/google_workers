import httplib2
from googleapiclient.discovery import build
from pydrive.drive import GoogleDrive
from google_workers.config import auth


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

    def get_folder_contents(self, folder_id):
        # a = ' and '
        query = f"'{folder_id}' in parents and trashed=false"
        params = {
            'q': query,
            'supportsAllDrives': True,
            'includeItemsFromAllDrives': True
        }
        return self.drive.ListFile(params).GetList()

    def upload_file(self, filepath, folder_id, new_title=None, team_drive_id=None, if_exists='raise'):
        upload_meta = {}
        if team_drive_id:
            upload_meta['parents'] = [{'id': folder_id, 'teamDriveId': team_drive_id}]
            upload_params = {'supportsTeamDrives': True}
        else:
            upload_meta['parents'] = [{'id': folder_id}]
            upload_params = None
        if new_title:
            upload_meta['title'] = new_title
        else:
            upload_meta['title'] = filepath.name

        files = self.get_folder_contents(folder_id)
        same_name_files = [i for i in files if i['title'] == upload_meta['title'] and 'downloadUrl' in i]
        if same_name_files:
            if if_exists == 'raise':
                raise AttributeError('На диске существует файл с таким же именем')
            elif if_exists == 'replace':
                for f in same_name_files:
                    f.Trash(upload_params)
                    if upload_params:
                        del upload_params['fileId']
            elif if_exists == 'ignore':
                pass
            else:
                raise TypeError('Неверный аргумент "if_exists"')

        file = self.drive.CreateFile(upload_meta)
        file.SetContentFile(str(filepath))
        file.Upload(upload_params)
        file.content.close()
        logger.debug('Uploaded!')
        return file['embedLink']

    def download_file(self, folder_id, filename, output_folder):
        files = self.get_folder_contents(folder_id)
        files_to_download = [i for i in files if i['title'] == filename and 'downloadUrl' in i]
        if len(files_to_download) > 1:
            logger.warning('На диске больше одного файла с таким названием')
        if files_to_download:
            for file in files_to_download:
                file.GetContentFile(output_folder / filename)
        else:
            raise TypeError(f'Не найден файл {filename} в папке {folder_id} на диске')

    def download_folder(self, folder_id, output_folder):
        files = self.get_folder_contents(folder_id)
        files_to_download = [i for i in files if 'downloadUrl' in i]
        if files_to_download:
            for file in files_to_download:
                file.GetContentFile(output_folder / file['title'])

    def get_file_link(self, folder_id, file_name):
        files = self.get_folder_contents(folder_id)
        needed_files = [i for i in files if i['title'] == file_name]
        if needed_files:
            return needed_files[0]['embedLink']
        else:
            logger.error(f'File {file_name} not found in folder {folder_id}!')