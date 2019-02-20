# Tescases for virasana.app.py
import unittest

from ajna_commons.flask.conf import MONGODB_URI
from bson.objectid import ObjectId
from pymongo import MongoClient

from virasana.models.models import Ocorrencias, Tags

conn = MongoClient(host=MONGODB_URI)
mongodb = conn['unit_test']


class ModelTestCase(unittest.TestCase):
    def setUp(self):
        self.inserted_id = \
            mongodb['fs.files'].insert_one({'teste': True}).inserted_id

    def tearDown(self):
        mongodb['fs.files'].delete_one({'_id': ObjectId(self.inserted_id)})

    def test1_add_ocorrencia(self):
        ocorrencias = Ocorrencias(mongodb)
        assert self.inserted_id is not None
        ocorrencias.add(self.inserted_id, 'ivan', 'Ocorrencia 1')
        ocorrencias.add(self.inserted_id, 'ivan', 'Ocorrencia 2')
        ocorrencias.add(self.inserted_id, 'ajna', 'Ocorrencia 3')
        lista_ocorrencias = ocorrencias.list(self.inserted_id)
        assert type(lista_ocorrencias) is list
        assert len(lista_ocorrencias) == 3

    def test2_list_ocorrencias_usuario(self):
        ocorrencias = Ocorrencias(mongodb)
        ocorrencias.add(self.inserted_id, 'ivan', 'Ocorrencia 1')
        ocorrencias.add(self.inserted_id, 'ivan', 'Ocorrencia 2')
        ocorrencias.add(self.inserted_id, 'ajna', 'Ocorrencia 3')
        lista_ocorrencias = ocorrencias.list_usuario(self.inserted_id, 'ivan')
        assert len(lista_ocorrencias) == 2
        lista_ocorrencias = ocorrencias.list_usuario(self.inserted_id, 'ajna')
        assert len(lista_ocorrencias) == 1
        assert lista_ocorrencias[0]['texto'] == 'Ocorrencia 3'
        assert lista_ocorrencias[0]['usuario'] == 'ajna'

    def test3_delete_ocorrencias(self):
        ocorrencias = Ocorrencias(mongodb)
        ocorrencias.add(self.inserted_id, 'ivan', 'Ocorrencia 1')
        ocorrencias.add(self.inserted_id, 'ivan', 'Ocorrencia 2')
        ocorrencias.add(self.inserted_id, 'ajna', 'Ocorrencia 3')
        lista_ocorrencias = ocorrencias.list_usuario(self.inserted_id, 'ajna')
        id_ocorrencia = lista_ocorrencias[0]['id_ocorrencia']
        print(id_ocorrencia)
        sucesso = ocorrencias.delete(self.inserted_id, id_ocorrencia)
        assert sucesso is True
        lista_ocorrencias = ocorrencias.list_usuario(self.inserted_id, 'ajna')
        assert lista_ocorrencias == []



    def test1_add_tag(self):
        tags = Tags(mongodb)
        assert self.inserted_id is not None
        tags.add(self.inserted_id, 'ivan', '1')
        tags.add(self.inserted_id, 'ivan', '2')
        tags.add(self.inserted_id, 'ajna', '3')
        lista_tags = tags.list(self.inserted_id)
        assert type(lista_tags) is list
        assert len(lista_tags) == 3

    def test2_list_tags_usuario(self):
        tags = Tags(mongodb)
        tags.add(self.inserted_id, 'ivan', '1')
        tags.add(self.inserted_id, 'ivan', '2')
        tags.add(self.inserted_id, 'ajna', '3')
        lista_tags = tags.list_usuario(self.inserted_id, 'ivan')
        assert len(lista_tags) == 2
        lista_tags = tags.list_usuario(self.inserted_id, 'ajna')
        assert len(lista_tags) == 1
        assert lista_tags == [{'tag': '3', 'usuario': 'ajna'}]

    def test3_delete_tags(self):
        tags = Tags(mongodb)
        tags.add(self.inserted_id, 'ivan', '1')
        tags.add(self.inserted_id, 'ivan', '2')
        tags.add(self.inserted_id, 'ajna', '3')
        sucesso = tags.delete(self.inserted_id, 'ajna', '3')
        assert sucesso is True
        lista_tags = tags.list_usuario(self.inserted_id, 'ajna')
        assert lista_tags == []

    def test4_tagged(self):
        inserted_id2 = mongodb['fs.files'].insert_one({'teste': True}).inserted_id
        try:
            tags = Tags(mongodb)
            tags.add(inserted_id2, 'ivan', '1')
            tags.add(inserted_id2, 'ivan', '2')
            tags.add(inserted_id2, 'ajna', '1')
            tags.add(self.inserted_id, '', '1')
            lista_tags = list(tags.tagged(usuario='space ghost'))
            assert lista_tags == []
            lista_tags = list(tags.tagged(tag='42'))
            assert lista_tags == []
            lista_tags = list(tags.tagged(tag='1'))
            assert len(lista_tags) == 2
            lista_tags = list(tags.tagged(tag='2'))
            assert len(lista_tags) == 1
        finally:
            mongodb['fs.files'].delete_one({'_id': ObjectId(inserted_id2)})
