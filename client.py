import logging
import os

from dotenv import load_dotenv
from webdav3.client import Client, WebDavException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("logclient")

load_dotenv()

options = {
    "webdav_hostname": os.getenv("HOST_URL"),
    "webdav_login":    os.getenv("USERNAME"),
    "webdav_password": os.getenv("PASSWORD"),
    "webdav_timeout":  240,
    "verbose": True,
}

def get_files(client: Client, remote_dir: str, local_dir:str):
    client.download_directory(remote_dir, local_dir)


def main():
    logger.info(f"Connecting to server {os.getenv("HOST_URL")}")
    try:
        client = Client(options)
        logger.info("Success")
    except Exception as e:
        logger.error(f"Could not connect, cause {e.args}")
        exit(-1)

    files = client.list("/eCallisto/bursts", get_info=True)
    for i in range(1, len(files)):
        # First entry is current dir
        file_info = files[i]
        if file_info["isdir"]:
            dir_name = file_info["path"].rstrip()
            print(f"{os.path.dirname(dir_name)}")

    logger.info("Downloading data")
    get_files(client, "/eCallisto/bursts/type_I", "e:\\Daten\\ecallisto")

if __name__ == "__main__":
    main()