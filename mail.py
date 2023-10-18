from simplegmail import Gmail
from google_workers.api import auth


class GoogleMailWorker:
    def __init__(self, credentials=None):
        self.creds = credentials if credentials is not None else auth()
        self.gmail = Gmail(_creds=self.creds)

    def send_email(self, to, subject, message):
        self.gmail.send_message(
            to=to,
            sender="litovchenkoao@pik.ru",
            subject=subject,
            msg_html=message,
        )