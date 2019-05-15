from pymongo import MongoClient

from dags.eosc.connection import create_connections
from dags.eosc.imaging import process_specimen_files
from dags.eosc.ml import BasicSVC
from dags.eosc.imaging import get_image_as_array


def test_images(cx='xray__ics'):
    create_connections()
    c = MongoClient()
    c.xray[cx].remove()
    process_specimen_files(cx)


def test_mongo():

    c = MongoClient()
    print(c.database_names())
    print(c.xray.collection_names())
    # print(len(list(c.xray.xray__ics.find())))
    print(c.xray.xray__ics.find_one({"_id": '43076_86819'}))


def test_train_ml(collection='xray'):

    clf = BasicSVC(collection)
    clf.train()
    # don't do this: test on the training set
    img = get_image_as_array('/opt/airflow/data/xray__ics/processed/42200_85273.bmp')
    print(clf.predict(img))


def test_load_ml(collection='xray'):

    clf = BasicSVC(collection)
    clf.load()
    # don't do this: test on the training set
    img = get_image_as_array('/opt/airflow/data/xray__ics/processed/42200_85273.bmp')
    print(clf.predict(img))


if __name__ == "__main__":
    # test_images()
    # test_mongo()

    test_train_ml()
    test_load_ml()