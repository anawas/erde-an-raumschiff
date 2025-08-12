import logging
import os
import sqlite3
from typing import List

from pathlib import Path
from dotenv import load_dotenv
from tqdm import trange
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

conn = sqlite3.connect("file_db.sqlite")
cur = conn.cursor()

def get_file_list(client: Client, remote_dir: str):
    files = client.list(remote_dir, get_info=True)
    cur.execute("CREATE TABLE files(name, size, downloaded)")
    get_file_list(client, remote_dir)

    for file in files:
        if file["isdir"] == False:
            cur.execute("INSERT INTO files VALUES (?, ?, ?)", (file["path"], int(file["size"]), False))
            conn.commit()

def update_db(id: str):
    cur1 = conn.cursor()
    cur1.execute("UPDATE files SET downloaded = 1 WHERE rowid = ?", (id,))
    conn.commit()
    cur1.close()


def get_files(client: Client, remote_dir: str, local_dir:str) -> None:
    """Downloads all the files in `remote_dir`

    Args:
        client (Client): The WebDAV client to use
        remote_dir (str): The folder path on remote server
        local_dir (str): Where to download the files
    """
    cur.execute("SELECT rowid, name, size, downloaded FROM files")
    while True:
        row = cur.fetchone()
        if row is None:
            break
        if row[3] == 1:
            continue
        id_ = row[0]
        filepath = Path(row[1])
        filename = filepath.name
        folder = filepath.parent.name
        # print(f"{os.path.join(remote_dir, folder, filename)} -> {os.path.join(local_dir, folder, filename)}")
        client.download_sync(os.path.join(remote_dir, folder, filename), os.path.join(local_dir, folder, filename))
        logger.info(f"Downloaded {filename}")
        update_db(id_)

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
        if file_info["isdir"] == True:
            dir_name: str = file_info["path"]
            # we get the full path up to the url. This also includes the
            # username of the account, which may vary. Thus, we
            # cut the path on the location where the remote_base_dir
            # can be found. This gives the true path. We then glue
            # the path togehter again.
            dir_name = dir_name.split(remote_base_dir)
            path_names.append(f"{remote_base_dir}{dir_name[1]}")
    return path_names

def main():
    logger.info(f"Connecting to server {os.getenv('HOST_URL')}")
    try:
        client = Client(options)
        logger.info("Success")
    except Exception as e:
        logger.error(f"Could not connect, cause {e.args}")
        exit(-1)

    # see wihich folders we have. They are probably named like "type_I"
    # logger.info("Getting folders on server...")
    # path_names = get_folders(client, "/eCallisto/bursts")
    # path = path_names[2]
    logger.info("Reading files to download from db...")
    get_files(client, "/eCallisto/bursts", "./data")

if __name__ == "__main__":
    main()
