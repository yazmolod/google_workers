from simplegmail import Gmail
from google_workers.api import auth


class GoogleMailWorker:
    def __init__(self):
        self.gmail = Gmail(_creds=auth())

    def send_email(self, to, subject, message):
        self.gmail.send_message(
            to=to,
            sender="litovchenkoao@pik.ru",
            subject=subject,
            msg_html=message,
        )