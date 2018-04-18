"""Unit tests para os módulos do pacote integração.

Cria banco de dados com dados fabricados para testes do pacote integracao no
método setUp. Limpa tudo no final no método tearDown. Chamar via pytest ou tox

Chamar python virasana/tests/integracao_test.py criará o Banco de Dados
SEM apagar tudo no final. Para inspeção visual do BD criado para testes.

"""
import os
import unittest
from datetime import datetime, timedelta

from gridfs import GridFS
from pymongo import MongoClient

from virasana.integracao import (DATA, carga, create_indexes, datas_bases,
                                 gridfs_count, peso_container_documento,
                                 stats_resumo_imagens, volume_container, xmli)

ZIP_DIR_TEST = os.path.join(os.path.dirname(__file__), 'sample')


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
        db['fs.files'].insert(
            {'metadata': {'numeroinformado': 'cheio',
                          'contentType': 'image/jpeg',
                          'recinto': 'A',
                          'dataescaneamento': data_escaneamento,
                          }})
        db['fs.files'].insert(
            {'metadata': {'numeroinformado': 'cheio2',
                          'contentType': 'image/jpeg',
                          'recinto': 'A',
                          'dataescaneamento': data_escaneamento,
                          }})
        db['fs.files'].insert(
            {'metadata': {'numeroinformado': 'vazio',
                          'contentType': 'image/jpeg',
                          'recinto': 'A',
                          'dataescaneamento': data_escaneamento}})
        db['fs.files'].insert(
            {'filename': 'comxmlS_stamp.jpg',
             'metadata': {'numeroinformado': 'comxml',
                          'contentType': 'image/jpeg',
                          'recinto': 'B',
                          'dataescaneamento': data_escaneamento}})
        db['fs.files'].insert(
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
        db['CARGA.Container'].insert(
            {'container': 'cheio',
             'conhecimento': 1,
             'pesobrutoitem': '10,00',
             'volumeitem': '1,00'})
        db['CARGA.Container'].insert(
            {'container': 'cheio2',
             'conhecimento': 2,
             'item': 1,
             'pesobrutoitem': '10,00',
             'volumeitem': '1,00'})
        db['CARGA.Container'].insert(
            {'container': 'cheio2',
             'conhecimento': 2,
             'item': 2,
             'pesobrutoitem': '10,00',
             'volumeitem': '1,00'})
        db['CARGA.Container'].insert(
            {'container': 'semconhecimento', 'conhecimento': 9})
        db['CARGA.Container'].insert(
            {'container': 'semescala', 'conhecimento': 3})
        db['CARGA.Container'].insert(
            {'container': 'escalaforadoprazo', 'conhecimento': 4})
        db['CARGA.ContainerVazio'].insert(
            {'container': 'vazio', 'manifesto': 2})
        db['CARGA.Conhecimento'].insert({'conhecimento': 1, 'tipo': 'mbl'})
        db['CARGA.Conhecimento'].insert({'conhecimento': 2, 'tipo': 'bl'})
        db['CARGA.Conhecimento'].insert({'conhecimento': 3, 'tipo': 'bl'})
        db['CARGA.Conhecimento'].insert({'conhecimento': 4, 'tipo': 'bl'})
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
        db['CARGA.Manifesto'].insert(
            {'manifesto': 1})
        db['CARGA.Manifesto'].insert(
            {'manifesto': 2})
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
        # carga.create_indexes(db)

    def tearDown(self):
        db = self.db
        db['fs.files'].drop()
        db['fs.chunks'].drop()
        db['CARGA.AtracDesatracEscala'].drop()
        db['CARGA.Escala'].drop()
        db['CARGA.EscalaManifesto'].drop()
        db['CARGA.Conhecimento'].drop()
        db['CARGA.ManifestoConhecimento'].drop()
        db['CARGA.Manifesto'].drop()
        db['CARGA.Container'].drop()
        db['CARGA.ContainerVazio'].drop()

    def test_indexes(self):
        create_indexes(self.db)
        xmli.create_indexes(self.db)
        carga.create_indexes(self.db)

    def test_count(self):
        assert gridfs_count(self.db) == 6

    def test_stats(self):
        stats = stats_resumo_imagens(self.db)
        assert stats is not None
        assert stats['Total de imagens'] == 5
        assert stats['Imagens com info do Carga'] == 0
        assert stats['Imagens com info do XML'] == 0
        assert stats['recinto']['A'] == 3
        assert stats['recinto']['B'] == 1
        assert stats['recinto']['C'] == 1

    def test_datas_bases(self):
        datas = datas_bases()
        assert datas is not None
        assert datas['gridfs'] == DATA
        assert datas['carga'] == carga.DATA
        assert datas['xml'] == xmli.DATA

    def test_busca_cheio(self):
        assert carga.busca_info_container(
            self.db, 'cheio', self.data_escaneamento_false) == {}
        cheio = carga.busca_info_container(
            self.db, 'cheio', self.data_escaneamento)
        print('CHEIO', cheio)
        assert cheio != {}
        assert cheio['vazio'] is False
        assert cheio['atracacao']['escala'] == 1
        assert cheio['container'][0]['conhecimento'] == 1

    def test_busca_vazio(self):
        assert carga.busca_info_container(
            self.db, 'vazio', self.data_escaneamento_false) == {}
        vazio = carga.busca_info_container(
            self.db, 'vazio', self.data_escaneamento)
        assert vazio != {}
        assert vazio['vazio'] is True
        assert vazio['atracacao']['escala'] == 2
        assert vazio['container'][0]['manifesto'] == 2

    def test_busca_semescala(self):
        assert carga.busca_info_container(
            self.db, 'semescala', self.data_escaneamento) == {}

    def test_busca_semconhecimento(self):
        assert carga.busca_info_container(
            self.db, 'semconhecimento', self.data_escaneamento) == {}

    def test_busca_foradoprazo(self):
        assert carga.busca_info_container(
            self.db, 'escalaforadoprazo', self.data_escaneamento) == {}

    def test_grava_fsfiles_carga(self):
        processados = carga.dados_carga_grava_fsfiles(
            self.db,
            data_inicio=self.data_escaneamento - timedelta(days=3))
        assert processados == 3
        semcarga = self.db['fs.files'].find({'metadata.carga': None}).count()
        assert semcarga == 3
        processados = carga.dados_carga_grava_fsfiles(
            self.db,
            data_inicio=self.data_escaneamento - timedelta(days=3))
        assert processados == 0

    def test_grava_fsfiles_xml(self):
        processados = xmli.dados_xml_grava_fsfiles(self.db)
        assert processados == 1
        semxml = self.db['fs.files'].find({'metadata.xml': None}).count()
        assert semxml == 5
        processados = xmli.dados_xml_grava_fsfiles(self.db)
        assert processados == 0

    def test_nlinhas_zip_dir(self):
        contador = carga.nlinhas_zip_dir(ZIP_DIR_TEST)
        print(contador)
        assert contador is not None
        assert contador['Alimento'] == 9
        assert contador['Esporte'] == 9

    def test_peso(self):
        # Primeiro processar carga para inserir peso
        carga.dados_carga_grava_fsfiles(
            self.db,
            data_inicio=self.data_escaneamento - timedelta(days=3))
        pesos = peso_container_documento(self.db, ['cheio', 'cheio2'])
        print('pesos', pesos)
        assert pesos['cheio'] == 10.0
        assert pesos['cheio2'] == 20.0

    def test_volume(self):
        # Primeiro processar carga para inserir peso
        carga.dados_carga_grava_fsfiles(
            self.db,
            data_inicio=self.data_escaneamento - timedelta(days=3))
        volumes = volume_container(self.db, ['cheio', 'cheio2'])
        print('VOLUMES', volumes)
        assert volumes['cheio'] == 1.0
        assert volumes['cheio2'] == 2.0


# Chamar python virasana/tests/integracao_test.py criará o Banco de Dados
# SEM apagar tudo no final. Para inspeção visual do BD criado para testes.
if __name__ == '__main__':
    print('Criando banco unit_test e Dados...')
    testdb = TestCase()
    testdb.setUp()
    carga.dados_carga_grava_fsfiles(
        testdb.db,
        data_inicio=testdb.data_escaneamento - timedelta(days=3))
