import os

LOCAL_DIR = "/opt/airflow/data"
RAW_IMAGE_SUBDIR = "raw"
PROCESSED_IMAGE_SUBDIR = "processed"

# nb of workers to process images
IMAGE_WORKERS = 10


def get_raw_image_dir(conn_id: str) -> str:
    """ The dir to store raw images for a connection """

    return os.path.join(LOCAL_DIR, conn_id, RAW_IMAGE_SUBDIR)


def get_processed_image_dir(conn_id: str) -> str:
    """ The dir to store processed images for a connection """

    return os.path.join(LOCAL_DIR, conn_id, PROCESSED_IMAGE_SUBDIR)