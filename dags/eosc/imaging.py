import os
import json
import logging
import numpy as np
import cv2

from typing import Dict
from glob import glob
from .connection import get_connection
from . import const
from pymongo import MongoClient


log = logging.getLogger(__name__)


def rotate(image, angle):
    # center
    (h, w) = image.shape[:2]
    (cX, cY) = (w / 2, h / 2)

    # grab the rotation matrix (applying the negative of the
    # angle to rotate clockwise), then grab the sine and cosine
    # (i.e., the rotation components of the matrix)
    M = cv2.getRotationMatrix2D((cX, cY), -angle, 1.0)
    cos = np.abs(M[0, 0])
    sin = np.abs(M[0, 1])

    # compute the new bounding dimensions of the image
    nW = int((h * sin) + (w * cos))
    nH = int((h * cos) + (w * sin))

    # adjust the rotation matrix to take into account translation
    M[0, 2] += (nW / 2) - cX
    M[1, 2] += (nH / 2) - cY

    # perform the actual rotation and return the image
    return cv2.warpAffine(image, M, (nW, nH))


def segmentize(image):

    # grayscale
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    # binary
    ret, thresh = cv2.threshold(gray, 5, 255, cv2.THRESH_BINARY)
    # dilation
    # kernel = np.ones((10, 1), np.uint8)
    # img_dilation = cv2.dilate(thresh, kernel, iterations=1)
    # contours
    ctrs, hierarchy = cv2.findContours(
        thresh.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )
    # keep largest contour
    rects = [cv2.boundingRect(ctr) for ctr in ctrs]
    rects = sorted(rects, key=lambda x: x[2])
    x, y, w, h = rects[-1]
    return image[y : y + h, x : x + w]


def _process_image(im: np.ndarray, config: Dict) -> np.ndarray:
    """ Image tranformations
    """

    angle = config["target_orientation"] - config["source_orientation"]
    if angle != 0:
        log.info("rotation %s deg", angle)
        im = rotate(im, angle)

    if config["source_colorscheme"] == "black_on_white":
        log.info("inversing colorscheme %s", source)
        im = cv2.bitwise_not(im)

    # log.info('conversion to %s', config["target_mode"])
    # im = im.convert(config["target_mode"])

    if config["segmentize"]:
        log.info("segmentize")
        im = segmentize(im)

    if config["target_mode"] == "gray":
        im = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)

    return im


def process_specimen_files(conn_id, *args, **kwargs):
    """ Process images to the desired state.

        :param conn_id: connection identifier to the remote server
    """

    datafiles = glob(os.path.join(const.LOCAL_DIR, conn_id, "*.json"))
    config = get_connection(conn_id).extra_dejson
    db = MongoClient()[config["collection"]][conn_id]
    raw_dir = const.get_raw_image_dir(conn_id)
    processed_dir = const.get_processed_image_dir(conn_id)

    for datafile in datafiles:
        with open(datafile, "r") as f_in:
            data = json.loads(f_in.read())
            for entry in data:
                source = os.path.join(raw_dir, entry["image"])
                log.info("processing %s", source)
                im = cv2.imread(source)
                im = _process_image(im, config)
                filename = "{}.{}".format(
                    os.path.splitext(entry["image"])[0], config["target_format"]
                )
                log.info("writing %s", filename)
                cv2.imwrite(os.path.join(processed_dir, filename), im)

                # save in mongo
                mongo_id = "{}_{}".format(
                    os.path.splitext(entry["image"])[0], config["target_format"]
                )
                entry["processed"] = filename
                db.find_one_and_update({"_id": mongo_id}, {"$set": entry}, upsert=True)


"""
Mongo:
from pymongo import MongoClient
c=MongoClient()
c.database_names()
c.xray.collection_names()
c.xray.xray__ics
len(list(c.xray.xray__ics.find()))
c.xray.xray__ics.find_one({"_id": '43076_86819'})
"""
