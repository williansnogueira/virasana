
"""Unit tests para os módulos do pacote integração."""
import unittest
from datetime import datetime

from gridfs import GridFS
from pymongo import MongoClient

from virasana.utils.auditoria import Auditoria


class TestCase(unittest.TestCase):
    def setUp(self):
        db = MongoClient()['unit_test']
        self.db = db
        # Cria data para testes
        data_escaneamento = datetime(2017, 1, 6)
        data_escalas = '05/01/2017'   # dois dias a menos
        data_escala_4 = '01/01/2017'  # cinco dias a menos
        data_escaneamento_false = datetime(2017, 1, 1)
        self.data_escaneamento = data_escaneamento
        self.data_escaneamento_false = data_escaneamento_false
        # Cria documentos fs.files simulando imagens para testes
        db['fs.files'].insert_one(
            {'metadata': {'numeroinformado': 'cheio',
                          'contentType': 'image/jpeg',
                          'recinto': 'A',
                          'dataescaneamento': data_escaneamento,
                          }})
        db['fs.files'].insert_one(
            {'metadata': {'numeroinformado': 'cheio2',
                          'contentType': 'image/jpeg',
                          'recinto': 'A',
                          'dataescaneamento': data_escaneamento,
                          }})
        db['fs.files'].insert_one(
            {'metadata': {'numeroinformado': 'vazio',
                          'contentType': 'image/jpeg',
                          'recinto': 'A',
                          'dataescaneamento': data_escaneamento}})
        db['fs.files'].insert_one(
            {'filename': 'comxmlS_stamp.jpg',
             'metadata': {'numeroinformado': 'comxml',
                          'contentType': 'image/jpeg',
                          'recinto': 'B',
                          'dataescaneamento': data_escaneamento}})
        db['fs.files'].insert_one(
            {'filename': 'semxmlS_stamp.jpg',
             'metadata': {'contentType': 'image/jpeg',
                          'recinto': 'C',
                          'dataescaneamento': data_escaneamento}})
        # Cria documentos simulando XML
        fs = GridFS(db)
        metadata = {'contentType': 'text/xml',
                    'dataescaneamento': data_escaneamento}
        fs.put(b'<DataForm><ContainerId>comxml</ContainerId></DataForm>',
               filename='comxml.xml', metadata=metadata)
        # Cria documentos simulando registros importados do CARGA
        db['CARGA.Container'].insert_one(
            {'container': 'cheio',
             'conhecimento': 1,
             'pesobrutoitem': '10,00',
             'volumeitem': '1,00'})
        db['CARGA.Container'].insert_one(
            {'container': 'cheio2',
             'conhecimento': 2,
             'item': 1,
             'pesobrutoitem': '10,00',
             'volumeitem': '1,00'})
        db['CARGA.Container'].insert_one(
            {'container': 'cheio2',
             'conhecimento': 2,
             'item': 2,
             'pesobrutoitem': '10,00',
             'volumeitem': '1,00'})
        db['CARGA.Container'].insert_one(
            {'container': 'semconhecimento', 'conhecimento': 9})
        db['CARGA.Container'].insert_one(
            {'container': 'semescala', 'conhecimento': 3})
        db['CARGA.Container'].insert_one(
            {'container': 'escalaforadoprazo', 'conhecimento': 4})
        db['CARGA.ContainerVazio'].insert_one(
            {'container': 'vazio', 'manifesto': 2})
        db['CARGA.Conhecimento'].insert_one({'conhecimento': 1})
        db['CARGA.Conhecimento'].insert_one({'conhecimento': 2})
        db['CARGA.Conhecimento'].insert_one({'conhecimento': 3})
        db['CARGA.Conhecimento'].insert_one({'conhecimento': 4})
        db['CARGA.ManifestoConhecimento'].insert_one(
            {'conhecimento': 1, 'manifesto': 1})
        db['CARGA.ManifestoConhecimento'].insert_one(
            {'conhecimento': 2, 'manifesto': 2})
        db['CARGA.ManifestoConhecimento'].insert_one(
            {'conhecimento': 3, 'manifesto': 3})
        db['CARGA.ManifestoConhecimento'].insert_one(
            {'conhecimento': 4, 'manifesto': 4})
        db['CARGA.ManifestoConhecimento'].insert_one(
            {'conhecimento': 3, 'manifesto': 32})
        db['CARGA.EscalaManifesto'].insert_one({'manifesto': 1, 'escala': 1})
        db['CARGA.EscalaManifesto'].insert_one({'manifesto': 2, 'escala': 2})
        db['CARGA.EscalaManifesto'].insert_one({'manifesto': 3, 'escala': 3})
        db['CARGA.EscalaManifesto'].insert_one({'manifesto': 4, 'escala': 4})
        db['CARGA.AtracDesatracEscala'].insert_one(
            {'escala': 4,
             'dataatracacao': data_escala_4,
             'horaatracacao': '00:00:01'})
        db['CARGA.AtracDesatracEscala'].insert_one(
            {'escala': 1, 'dataatracacao': data_escalas,
             'horaatracacao': '00:00:01'})
        db['CARGA.AtracDesatracEscala'].insert_one(
            {'escala': 2, 'dataatracacao': data_escalas,
             'horaatracacao': '00:00:01'})

    def tearDown(self):
        db = self.db
        db['fs.files'].drop()
        db['fs.chunks'].drop()
        db['CARGA.AtracDesatracEscala'].drop()
        db['CARGA.Escala'].drop()
        db['CARGA.EscalaManifesto'].drop()
        db['CARGA.Conhecimento'].drop()
        db['CARGA.ManifestoConhecimento'].drop()
        db['CARGA.Container'].drop()
        db['CARGA.ContainerVazio'].drop()

    def test_auditoria(self):
        auditor = Auditoria(self.db)
        auditor.add_relatorio('nome', {})
