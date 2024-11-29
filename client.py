import os
from webdav3.client import Client
from dotenv import load_dotenv
import logging

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
        print(f"{file_info["path"]}")

