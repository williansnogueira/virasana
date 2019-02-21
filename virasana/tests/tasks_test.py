import datetime
import os
import unittest

import ajna_commons.flask.login as login_ajna
import gridfs
import pytest
from ajna_commons.flask.conf import BACKEND, BROKER
from ajna_commons.flask.conf import MONGODB_URI
from ajna_commons.models.bsonimage import BsonImage, BsonImageList
from pymongo import MongoClient

from virasana.views import configure_app
from virasana.workers.tasks import celery

TEST_BSON = os.path.join(os.path.dirname(
    __file__), 'test.bson')
TEST_PATH = os.path.abspath(os.path.dirname(__file__))
IMG_FOLDER = os.path.join(TEST_PATH)

files_ids = None


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': BROKER,
        'result_backend': BACKEND
    }


@pytest.fixture(scope='session')
def celery_parameters():
    return {
        'task_cls': celery.task_cls,
        'strict_typing': False,
    }


class FlaskCeleryBsonTestCase(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def init_worker(self, celery_worker):
        self.worker = celery_worker

    def setUp(self):
        conn = MongoClient(host=MONGODB_URI)
        mongodb = conn['unit_test']
        app = configure_app(mongodb)
        # Aceitar autenticação com qualquer username == password
        login_ajna.DBUser.dbsession = None
        app.testing = True
        self.app = app.test_client()
        self._bsonimage = BsonImage(
            filename=os.path.join(IMG_FOLDER, 'stamp1.jpg'),
            chave='virasana1',
            origem=0,
            data=datetime.datetime.utcnow()
        )
        self._bsonimage2 = BsonImage(
            filename=os.path.join(IMG_FOLDER, 'stamp2.jpg'),
            chave='virasana2',
            origem=1,
            data=datetime.datetime.utcnow()
        )
        bsonimagelist = BsonImageList()
        bsonimagelist.addBsonImage(self._bsonimage)
        bsonimagelist.addBsonImage(self._bsonimage2)
        bsonimagelist.tofile(TEST_BSON)
        self._db = MongoClient().unit_test
        self._fs = gridfs.GridFS(self._db)

    def tearDown(self):
        # os.remove(TEST_BSON)
        files = self._fs.find({'metadata.chave': 'virasana1'})
        for file in files:
            self._fs.delete(file._id)
        files = self._fs.find({'metadata.chave': 'virasana2'})
        for file in files:
            self._fs.delete(file._id)

    """
    def test_apiupload(self):
        self.app.post('/login', data=dict(
            username='ajna',
            senha='ajna',
        ), follow_redirects=True)
        bson = open(TEST_BSON, 'rb').read()
        data = {}
        data['file'] = (BytesIO(bson), 'test.bson')
        rv = self.app.post(
            '/uploadbson', content_type='multipart/form-data', data=data)
        print(rv.data)
        files = {'file': bson}
        rv = self.app.post(
            '/api/uploadbson', data=files)
        print(rv.data)
        assert rv.data is not None
    """
