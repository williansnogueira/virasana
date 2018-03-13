import unittest
from datetime import datetime
from pymongo import MongoClient

from virasana.integracao.carga import busca_info_container, \
    dados_carga_grava_fsfiles


class FlaskTestCase(unittest.TestCase):
    def setUp(self):
        db = MongoClient()['unit_test']
        self.db = db
        data_escaneamento = datetime(2017, 1, 6)
        data_escalas = '05/01/2017'   # dois dias a menos
        data_escala_4 = '01/01/2017'  # cinco dias a menos
        data_escaneamento_false = datetime(2017, 1, 1)
        self.data_escaneamento = data_escaneamento
        self.data_escaneamento_false = data_escaneamento_false
        db['fs.files'].insert(
            {'metadata': {'numeroinformado': 'cheio'},
             'metadata': {'dataescaneamento': data_escaneamento}})
        db['fs.files'].insert(
            {'metadata': {'numeroinformado': 'vazio'},
             'metadata': {'dataescaneamento': data_escaneamento}})
        db['CARGA.Container'].insert({'container': 'cheio', 'conhecimento': 1})
        db['CARGA.Container'].insert(
            {'container': 'semconhecimento', 'conhecimento': 9})
        db['CARGA.Container'].insert(
            {'container': 'semescala', 'conhecimento': 3})
        db['CARGA.Container'].insert(
            {'container': 'escalaforadoprazo', 'conhecimento': 4})
        db['CARGA.ContainerVazio'].insert(
            {'container': 'vazio', 'manifesto': 2})
        db['CARGA.Conhecimento'].insert({'conhecimento': 1})
        db['CARGA.Conhecimento'].insert({'conhecimento': 2})
        db['CARGA.Conhecimento'].insert({'conhecimento': 3})
        db['CARGA.Conhecimento'].insert({'conhecimento': 4})
        db['CARGA.ManifestoConhecimento'].insert(
            {'conhecimento': 1, 'manifesto': 1})
        db['CARGA.ManifestoConhecimento'].insert(
            {'conhecimento': 2, 'manifesto': 2})
        db['CARGA.ManifestoConhecimento'].insert(
            {'conhecimento': 3, 'manifesto': 3})
        db['CARGA.ManifestoConhecimento'].insert(
            {'conhecimento': 4, 'manifesto': 4})
        db['CARGA.ManifestoConhecimento'].insert(
            {'conhecimento': 3, 'manifesto': 32})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 1, 'escala': 1})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 2, 'escala': 2})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 3, 'escala': 3})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 4, 'escala': 4})
        db['CARGA.AtracDesatracEscala'].insert(
            {'escala': 4,
             'dataatracacao': data_escala_4,
             'horaatracacao': '00:00:01'})
        db['CARGA.AtracDesatracEscala'].insert(
            {'escala': 1, 'dataatracacao': data_escalas,
             'horaatracacao': '00:00:01'})
        db['CARGA.AtracDesatracEscala'].insert(
            {'escala': 2, 'dataatracacao': data_escalas,
             'horaatracacao': '00:00:01'})

    def tearDown(self):
        db = self.db
        db['fs.files'].drop()
        db['CARGA.AtracDesatracEscala'].drop()
        db['CARGA.Escala'].drop()
        db['CARGA.EscalaManifesto'].drop()
        db['CARGA.Conhecimento'].drop()
        db['CARGA.ConhecimentoManifesto'].drop()
        db['CARGA.Container'].drop()
        db['CARGA.ContainerVazio'].drop()

    def test_busca_cheio(self):
        assert busca_info_container(
            self.db, 'cheio', self.data_escaneamento_false) == {}
        cheio = busca_info_container(self.db, 'cheio', self.data_escaneamento)
        assert cheio != {}
        assert cheio['vazio'] == False
        assert cheio['atracacao']['escala'] == 1
        assert cheio['container'][0]['conhecimento'] == 1

    def test_busca_vazio(self):
        assert busca_info_container(
            self.db, 'vazio', self.data_escaneamento_false) == {}
        vazio = busca_info_container(self.db, 'vazio', self.data_escaneamento)
        assert vazio != {}
        assert vazio['vazio'] == True
        assert vazio['atracacao']['escala'] == 2
        assert vazio['container'][0]['manifesto'] == 2

    def test_busca_semescala(self):
        assert busca_info_container(
            self.db, 'semescala', self.data_escaneamento) == {}

    def test_busca_semconhecimento(self):
        assert busca_info_container(
            self.db, 'semconhecimento', self.data_escaneamento) == {}

    def test_busca_semconhecimento(self):
        assert busca_info_container(
            self.db, 'escalaforadoprazo', self.data_escaneamento) == {}
