# Tescases for virasana.app.py
import os
import unittest

import ajna_commons.flask.login as login_ajna
from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from bson.objectid import ObjectId
from pymongo import MongoClient

from virasana.views import configure_app

conn = MongoClient(host=MONGODB_URI)
mongodb = conn['unit_test']
app = configure_app(mongodb)
# Aceitar autenticação com qualquer username == password
login_ajna.DBUser.dbsession = None


class FlaskTestCase(unittest.TestCase):
    def setUp(self):
        # Ativar esta variável de ambiente na inicialização
        # do Servidor WEB para transformar em teste de integração
        self.http_server = os.environ.get('HTTP_SERVER')
        if self.http_server is not None:
            from webtest import TestApp
            self.app = TestApp(self.http_server)
        else:
            app.testing = True
            self.app = app.test_client()

    def tearDown(self):
        self.logout()

    def get_token(self, url):
        if self.http_server is not None:
            response = self.app.get(url)
            self.csrf_token = str(response.html.find_all(
                attrs={'name': 'csrf_token'})[0])
            begin = self.csrf_token.find('value="') + 7
            end = self.csrf_token.find('"/>')
            self.csrf_token = self.csrf_token[begin: end]
        else:
            response = self.app.get(url, follow_redirects=True)
            csrf_token = response.data.decode()
            begin = csrf_token.find('csrf_token"') + 10
            end = csrf_token.find('username"') - 10
            csrf_token = csrf_token[begin: end]
            begin = csrf_token.find('value="') + 7
            end = csrf_token.find('/>')
            self.csrf_token = csrf_token[begin: end]
            return self.csrf_token

    def login(self, username, senha):
        self.get_token('/login')
        self.http_server = None
        if self.http_server is not None:
            response = self.app.post('/login',
                                     params=dict(
                                         username=username,
                                         senha=senha,
                                         csrf_token=self.csrf_token)
                                     )
            return response
        else:
            return self.app.post('/login', data=dict(
                username=username,
                senha=senha,
                csrf_token=self.csrf_token
            ), follow_redirects=True)

    def logout(self):
        return self._get('/logout', follow_redirects=True)

    # methods
    def data(self, rv):
        if self.http_server is not None:
            return str(rv.html).encode('utf_8')
        return rv.data

    def _post(self, url, data, follow_redirects=True):
        self.get_token(url)
        data['csrf_token'] = self.csrf_token
        print('TOKEN', self.csrf_token)
        if self.http_server is not None:
            rv = self.app.post(url, params=data)
        else:
            rv = self.app.post(url, data=data,
                               follow_redirects=follow_redirects)
        return rv

    def _get(self, url, follow_redirects=True):
        if self.http_server is not None:
            rv = self.app.get(url)
        else:
            rv = self.app.get(url,
                              follow_redirects=follow_redirects)
        return rv

    def test_not_found(self):
        rv = self._get('/non_ecsiste')
        assert b'Erro 404' in rv.data

    def test_index(self):
        rv = self._get('/', follow_redirects=True)
        data = self.data(rv)
        assert b'AJNA' in data
        assert b'input type="password"' in data
        self.login('ajna', 'ajna')
        rv = self._get('/', follow_redirects=True)
        data = self.data(rv)
        assert b'AJNA' in data

    def test_upload_bson(self):
        self.login('ajna', 'ajna')
        rv = self._get('/uploadbson', follow_redirects=True)
        assert b'AJNA' in rv.data
        print(rv.data)

    def test_task_progress(self):
        self.login('ajna', 'ajna')
        rv = self._get('/api/task/123')
        assert b'state' in rv.data
        print(rv.data)

    def test_list(self):
        self.login('ajna', 'ajna')
        rv = self._get('/list_files')
        assert b'AJNA' in rv.data
        print(rv.data)
        # TODO: insert file and test return on list

    def test_file(self):
        self.login('ajna', 'ajna')
        # rv = self._get('/file/123')
        # assert b'AJNA' in rv.data
        # print(rv.data)

    def test_image(self):
        self.login('ajna', 'ajna')
        # rv = self._get('/image/123')
        # assert b'AJNA' in rv.data
        # print(rv.data)

    def test_files(self):
        self.login('ajna', 'ajna')
        rv = self._get('/files')
        assert b'AJNA' in rv.data
        print(rv.data)

    def test_tags_usuario(self):
        _id = mongodb['fs.files'].insert_one({'teste': True}).inserted_id
        try:

            self.login('ajna', 'ajna')
            data = {'_id': str(_id),
                    'tag': '3'}
            rv = self.app.post('/tag/add', data=data,
                               follow_redirects=False)
            assert rv.is_json
            rvjson = rv.get_json()
            assert rvjson.get('success') is True
        finally:
            mongodb['fs.files'].delete_one({'_id': ObjectId(_id)})


    def test_ocorrencias_usuario(self):
        _id = mongodb['fs.files'].insert_one({'teste': True}).inserted_id
        try:

            self.login('ajna', 'ajna')
            data = {'_id': str(_id),
                    'texto': '3'}
            rv = self.app.post('/ocorrencia/add', data=data,
                               follow_redirects=False)
            assert rv.is_json
            rvjson = rv.get_json()
            assert rvjson.get('success') is True
        finally:
            mongodb['fs.files'].delete_one({'_id': ObjectId(_id)})
