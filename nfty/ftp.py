import logging
from io import BytesIO
import pysftp

logger = logging.getLogger("AppLogger")


class Sftp:
    def __init__(self, hostname, username, password, port=22):
        self.connection = None
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.file = BytesIO()

    def connect(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.connection = pysftp.Connection(
            host=self.hostname,
            username=self.username,
            password=self.password,
            port=self.port,
            cnopts=cnopts,
        )

    def disconnect(self):
        self.connection.close()

    def listdir(self, remote_path):
        return self.connection.listdir(remote_path)

    def listdir_attr(self, remote_path):
        return self.connection.listdir_attr(remote_path)

    def download(self, remote_path):
        self.connection.getfo(remote_path, self.file)
