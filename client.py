import logging
import os
from typing import List

from dotenv import load_dotenv
from webdav3.client import Client

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

def get_files(client: Client, remote_dir: str, local_dir:str) -> None:
    """Downloads all the files in `remote_dir`

    Args:
        client (Client): The WebDAV client to use
        remote_dir (str): The folder path on remote server
        local_dir (str): Where to download the files
    """
    def callback(current, total):
        if current == 0:
            print(f"Downloading {total} bytes")
        
    client.download_directory(remote_dir, local_dir, progress=callback)

def get_folders(client: Client, remote_base_dir: str) -> List[str]:
    """Gets a list of path names where burst image can be found¨
       This assumes that 

    Args:
        client (Client): The WebDAV client to use
        remote_base_dir (str): The base directory to search the folders in

    Returns:
        List[str]: List of path names
    """
    path_names = []
    files = client.list(remote_base_dir, get_info=True)
    for i in range(1, len(files)):
        # First entry is current dir
        file_info = files[i]
        if file_info["isdir"]:
            dir_name: str = file_info["path"]
            # we get the full path up to the url. This also includes the
            # username of the account, which may vary. Thus, we
            # cut the path on the location where the remote_base_dir
            # can be found. This gives the true path. We then clue
            # the path togehter again.
            dir_name = dir_name.split(remote_base_dir)
            path_names.append(f"{remote_base_dir}{dir_name[1]}")
    return path_names

def main():
    logger.info(f"Connecting to server {os.getenv("HOST_URL")}")
    try:
        client = Client(options)
        logger.info("Success")
    except Exception as e:
        logger.error(f"Could not connect, cause {e.args}")
        exit(-1)

    # see wihich folders we have. They are probably named like "type_I" 
    path_names = get_folders(client, "/eCallisto/bursts")

    logger.info("Downloading data")
    for path in path_names:
        last_folder = path.split("/")[-2]
        get_files(client, path, f"e:\\ecallisto\\{last_folder}")

if __name__ == "__main__":
    main()