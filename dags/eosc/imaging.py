import os
import json

from PIL import Image
from glob import glob
from ..eosc.connection import get_connection

LOCAL_DIR = "/opt/airflow/data"


def process_specimen_files(conn_id, *args, **kwargs):
    """ Process images to the desired state.

        :param conn_id: connection identifier to the remote server
    """

    datafiles = glob(os.path.join(LOCAL_DIR, conn_id, "*.json"))
    config = get_connection(conn_id).extra_dejson

    for datafile in datafiles:
        with open(datafile, 'r') as f_in:
            data = json.loads(f_in.read())
            for entry in data:
                im = Image.open(os.path.join(LOCAL_DIR, conn_id, entry["image"]))
                if config["source_orientation"] != config["target_orientation"]:
                    im.rotate(config["target_orientation"] - config["source_orientation"])
                im = im.convert(config["target_mode"])
                im.save("{}.{}".format(os.path.splitext(entry["image"][0]), config["target_format"]))
