import os
import json

from airflow.contrib.hooks.ftp_hook import FTPHook

LOCAL_DIR = "/opt/airflow/data"

def load_specimen_files(conn_id, *args, **kwargs):
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
                        dst = os.path.join(LOCAL_DIR, conn_id, entry["image"])
                        ftp.retrieve_file(entry["image"], dst)

    return datafiles