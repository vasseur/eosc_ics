import os
import json
import logging
import numpy as np
import cv2

from typing import Dict, Tuple
from glob import glob
from .connection import get_connection
from . import const
from pymongo import MongoClient


log = logging.getLogger(__name__)


def rotate(image: np.ndarray, params: Dict) -> np.ndarray:

    angle = params["angle"]
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


def segmentize(image: np.ndarray, params: Dict) -> np.ndarray:

    # grayscale if required
    try:
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    except:
        gray = image

    h, w = image.shape[:2]
    x1 = 0
    y1 = 0
    x2 = w
    y2 = h

    if "roi" in params:
        # analyse only roi
        x1, y1, x2, y2 = params["roi"]
        x1 = x1 or 0
        y1 = y1 or 0
        x2 = x2 or w
        y2 = y2 or h

    roi = gray[y1 : y2, x1 : x2]

    # binary
    ret, thresh = cv2.threshold(roi, 5, 255, cv2.THRESH_BINARY)
    # dilation
    # kernel = np.ones((10, 1), np.uint8)
    # img_dilation = cv2.dilate(thresh, kernel, iterations=1)
    # contours
    ctrs, hierarchy = cv2.findContours(
        thresh.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )
    # keep largest contour
    ctr = sorted(ctrs, key = cv2.contourArea, reverse = True)[0]
    xr, yr, wr, hr = cv2.boundingRect(ctr)

    # take full image into account
    xr += x1
    yr += y1

    # expand to keep same size for all images
    if 'expand_horizontal' in params:
        expand_h = params['expand_horizontal']["size"]
        direction = params['expand_horizontal']["direction"]
        delta = expand_h - wr
        wr += delta
        if direction == "left":
            xr = max(xr - delta, 0)
        elif direction == "right":
            y2 = min(xr + wr, w)
            xr = y2 - wr
        elif direction == "both":
            xr = max(xr - int(delta / 2), 0)
    if 'expand_vertical' in params:
        expand_v = params['expand_vertical']["size"]
        direction = params['expand_vertical']["direction"]
        delta = expand_v - hr
        hr += delta
        if direction == "top":
            yr =max( yr - delta, 0)
        elif direction == "both":
            yr = max(yr - int(delta / 2), 0)

    return image[yr : yr + hr, xr : xr + wr]


def crop(image: np.ndarray, params: Dict) -> np.ndarray:

    x1, y1, x2, y2 = params["area"]
    h, w = image.shape[:2]
    x1 = x1 or 0
    x2 = x2 or w
    y1 = y1 or 0
    y2 = y2 or h
    return image[y1:y2, x1:x2]


def convert(image: np.ndarray, params: Dict) -> np.ndarray:

    if params["mode"] == "gray":
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return image


def invert(image: np.ndarray, params: Dict) -> np.ndarray:

    return cv2.bitwise_not(image)


def resize(image: np.ndarray, params: Dict) -> np.ndarray:

    h, w = image.shape[:2]
    if "ratio" in params:
        ratio = params["ratio"]
        w2, h2 = int(w * ratio), int(h * ratio)
    elif "width" in params:
        ratio = params["width"] / w
        w2, h2 = params["width"], int(h * ratio)
    elif "height" in params:
        ratio = params["height"] / h
        w2, h2 = int(w * ratio), params["height"]
    return cv2.resize(image, (w2, h2), interpolation = cv2.INTER_AREA)


def _process_image(image: np.ndarray, config: Dict) -> np.ndarray:
    """ Image tranformations

        :param config: extra param in connection
    """

    for (name, params) in config["pipeline"]:
        func = globals()[name]
        log.info('%s with %s', name, str(params))
        image = func(image, params)
    return image


def get_image_as_array(filename: str) -> np.array:

    im = cv2.imread(filename, cv2.IMREAD_GRAYSCALE)
    return im.reshape(im.size)


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
                basename = os.path.splitext(entry["image"])[0]
                filename = "{}.{}".format(basename, config["target_format"])
                log.info("writing %s", filename)
                cv2.imwrite(os.path.join(processed_dir, filename), im)
                # save in mongo
                entry["processed"] = filename
                db.find_one_and_update({"_id": basename}, {"$set": entry}, upsert=True)

