import os
import numpy as np
import cv2
import joblib

from typing import List
from sklearn import svm
from pymongo import MongoClient
from .connection import get_connection, get_connections
from .const import get_processed_image_dir, LOCAL_DIR
from .imaging import get_image_as_array


class BasicSVC:

    def __init__(self, collection: str):

        self._collection = collection
        self._clf = None

    def _get_data(self) -> List:

        c = MongoClient()
        X = []
        Y = []
        for cx in get_connections(self._collection):
            data = c[self._collection][cx.conn_id].find()
            for row in data:
                filename = os.path.join(get_processed_image_dir(cx.conn_id), row['processed'])
                X.append(get_image_as_array(filename))
                Y.append(row['sex'])
        return np.array(X), np.array(Y) #.reshape((-1,1))

    def train(self):

        X, y = self._get_data()
        self._clf = svm.SVC(gamma='scale')
        self._clf.fit(X, y)

    def predict(self, image):

        return self._clf.predict([image])[0]

    def _get_filename(self):

        return os.path.join(LOCAL_DIR, '{}.joblib'.format(self._collection))

    def load(self):

        self._clf = joblib.load(self._get_filename())

    def save(self):

        joblib.dump(self._clf, self._get_filename())


def train_model(collection: str):

    svc = BasicSVC(collection)
    svc.train()
    svc.save()


def get(collection: str):

    svc = BasicSVC(collection)
    svc.load()
    return svc