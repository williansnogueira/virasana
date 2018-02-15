# Tescases for virasana.app.py
import os
import unittest

from virasana.app import app


class FlaskTestCase(unittest.TestCase):
    def setUp(self):
        self.http_server = os.environ.get('HTTP_SERVER')
        if self.http_server is not None:
            from webtest import TestApp
            self.app = TestApp(self.http_server)
        else:
            app.testing = True
            app.config['WTF_CSRF_ENABLED'] = False
            self.app = app.test_client()
        rv = self.login('ajna', 'ajna')
        assert rv is not None

    def tearDown(self):
        rv = self.logout()
        assert rv is not None

    def login(self, username, senha):
        if self.http_server is not None:
            # First, get the CSRF Token
            response = self.app.get('/login')
            self.csrf_token = str(response.html.find_all(
                attrs={'name': 'csrf_token'})[0])
            begin = self.csrf_token.find('value="') + 7
            end = self.csrf_token.find('"/>')
            self.csrf_token = self.csrf_token[begin: end]
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
                senha=senha
            ), follow_redirects=True)

    def logout(self):
        if self.http_server is not None:
            return self.app.get('/logout',
                                params=dict(csrf_token=self.csrf_token))
        else:
            return self.app.get('/logout', follow_redirects=True)

    def test_index(self):
        rv = self.app.get('/', follow_redirects=True)
        assert b'AJNA' in rv.data

    def test_list(self):
        rv = self.app.get('/list_files', follow_redirects=True)
        assert b'AJNA' in rv.data
        # TODO: insert file and test return on list
