import os
import json

from typing import List
from . import const
from airflow.contrib.hooks.ftp_hook import FTPHook

LOCAL_DIR = "/opt/airflow/data"

def load_specimen_files(conn_id: str, *args, **kwargs) -> List[str]:
    """ Get the specimen files from a remote ftp site.
        These files should have the pattern specimen*.json.
        Load images associated.

        :param conn_id: connection identifier to the remote server
        :return: list of file contents
    """

    dst = os.path.join(LOCAL_DIR, conn_id)
    if not os.path.exists(dst):
        os.makedirs(dst)

    with FTPHook(ftp_conn_id=conn_id) as ftp:
        config = ftp.get_connection(conn_id).extra_dejson
        datafiles = []
        raw_dir = const.get_raw_image_dir(conn_id)

        files = ftp.list_directory(config["source_directory"])
        # nb: list_directory does a cwd ...
        datafiles = [f for f in files if f.endswith('.json')]

        for fic in datafiles:
            if fic.endswith('.json'):
                dst = os.path.join(LOCAL_DIR, conn_id, fic)
                ftp.retrieve_file(fic, dst)
                with open(dst, 'r') as f_in:
                    data = json.loads(f_in.read())
                    for entry in data:
                        dst = os.path.join(raw_dir, entry["image"])
                        ftp.retrieve_file(entry["image"], dst)

    return datafiles


def create_dirs(conn_id: str, *args, **kwargs) -> None:
    """ Create subdirs to store images """

    raw = const.get_raw_image_dir(conn_id)
    if not os.path.exists(raw):
        os.makedirs(raw)

    processed = const.get_processed_image_dir(conn_id)
    if not os.path.exists(processed):
        os.makedirs(processed)

