"""Unit tests para os módulos do pacote integração.padma.

"""
import os
import unittest

from virasana.integracao.padma import consulta_padma, login

TEST_IMAGE = os.path.join(os.path.dirname(__file__), 'stamp1.jpg')


class TestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_login(self):
        r = login('ajna', 'ajna')
        assert r is not None

    def test_consulta_modelo(self):
        image = open(TEST_IMAGE, 'rb')
        r = consulta_padma(image, 'vazio')
        print(r)
        assert r is not None
        assert isinstance(r, dict)
        assert r.get('success') is True
        # print(r.text)
