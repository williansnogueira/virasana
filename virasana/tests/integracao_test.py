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

from virasana.integracao import (atualiza_stats, DATA, carga, create_indexes,
                                 datas_bases, dict_to_text, dict_to_html,
                                 gridfs_count, peso_container_documento,
                                 plot_bar_plotly, plot_pie_plotly,
                                 stats_resumo_imagens, summary,
                                 volume_container, xmli)

ZIP_DIR_TEST = os.path.join(os.path.dirname(__file__), 'sample')


class TestCase(unittest.TestCase):
    def setUp(self):
        db = MongoClient()['unit_test']
        self.db = db
        # Cria data para testes
        data_escaneamento = datetime(2017, 1, 6)
        # Data de escaneamento do contêiner de exportação
        # Dois dias depois da entrada do manifesto de vazio
        # O contêiner de exportação entra como vazio, dois dias
        # após é escaneado novamente, e oito dias depois sai
        # em CE de exportação
        data_escaneamento_cheioe = datetime(2017, 1, 8)
        data_escaneamento_menos2 = '05/01/2017'  # dois dias a menos
        data_escaneamento_menos4 = '03/01/2017'  # quatro dias a menos
        data_escaneamento_menos6 = '01/01/2017'  # seis dias a menos
        data_escaneamento_mais8 = '16/01/2017'  # oito dias a mais
        data_escaneamento_false = datetime(2016, 12, 1)
        self.data_escaneamento = data_escaneamento
        self.data_escaneamento_cheioe = data_escaneamento_cheioe
        self.data_escaneamento_false = data_escaneamento_false
        # Cria documentos fs.files simulando imagens para testes
        # São dois contêineres cheios de importação
        # Um manifesto de vazio
        # Um contêiner cheio de exportação que TAMBÈM
        # passou num manifesto de vazio dois dias antes
        db['fs.files'].insert(
            {'uploadDate': data_escaneamento,
             'metadata': {'numeroinformado': 'cheio',

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
            {'metadata': {'numeroinformado': 'cheioe',
                          'contentType': 'image/jpeg',
                          'recinto': 'A',
                          'dataescaneamento': data_escaneamento,
                          }})
        db['fs.files'].insert(
            {'metadata': {'numeroinformado': 'cheioe',
                          'contentType': 'image/jpeg',
                          'recinto': 'A',
                          'dataescaneamento': data_escaneamento,
                          }})
        db['fs.files'].insert(
            {'filename': 'semxmlS_stamp.jpg',
             'metadata': {'contentType': 'image/jpeg',
                          'recinto': 'C',
                          'dataescaneamento': data_escaneamento}})
        # Cria documentos simulando XML
        fs = GridFS(db)
        metadata = {'contentType': 'text/xml',
                    'dataescaneamento': data_escaneamento}
        fs.put(b'<DataForm><Login>IvanSB</Login>' +
               b'<ContainerId>comxml</ContainerId></DataForm>',
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
            {'container': 'cheioe',
             'conhecimento': 21,
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
        # Condição em que existe tanto manifesto de vazio quanto
        # CE de cheio - sistema deve buscar data mais próxima
        db['CARGA.ContainerVazio'].insert(
            {'container': 'cheio', 'manifesto': 41})
        # Aqui contêiner de exportação entrou!!!!
        db['CARGA.ContainerVazio'].insert(
            {'container': 'cheioe', 'manifesto': 22})
        db['CARGA.Conhecimento'].insert({'conhecimento': 1, 'tipo': 'mbl'})
        db['CARGA.Conhecimento'].insert({'conhecimento': 2, 'tipo': 'bl'})
        db['CARGA.Conhecimento'].insert({'conhecimento': 3, 'tipo': 'bl'})
        db['CARGA.Conhecimento'].insert({'conhecimento': 4, 'tipo': 'bl'})
        db['CARGA.Conhecimento'].insert({'conhecimentoe': 5, 'tipo': 'bl'})
        db['CARGA.Conhecimento'].insert({'conhecimento': 21, 'tipo': 'mbl'})
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
        db['CARGA.ManifestoConhecimento'].insert(
            {'conhecimento': 21, 'manifesto': 21})
        db['CARGA.Manifesto'].insert(
            {'manifesto': 1, 'tipomanifesto': 'lci'})
        db['CARGA.Manifesto'].insert(
            {'manifesto': 2, 'tipomanifesto': 'lci'})
        db['CARGA.Manifesto'].insert(
            {'manifesto': 21, 'tipomanifesto': 'lce'})
        db['CARGA.Manifesto'].insert(
            {'manifesto': 22, 'tipomanifesto': 'bce'})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 1, 'escala': 1})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 2, 'escala': 2})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 3, 'escala': 3})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 4, 'escala': 4})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 41, 'escala': 41})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 21, 'escala': 21})
        db['CARGA.EscalaManifesto'].insert({'manifesto': 22, 'escala': 22})
        db['CARGA.AtracDesatracEscala'].insert(
            {'escala': 4,
             'dataatracacao': data_escaneamento_menos6,
             'horaatracacao': '00:00:01'})
        db['CARGA.AtracDesatracEscala'].insert(
            {'escala': 41,
             'dataatracacao': data_escaneamento_menos2,
             'horaatracacao': '00:00:01'})
        db['CARGA.AtracDesatracEscala'].insert(
            {'escala': 1, 'dataatracacao': data_escaneamento_menos2,
             'horaatracacao': '00:00:01'})
        db['CARGA.AtracDesatracEscala'].insert(
            {'escala': 2, 'dataatracacao': data_escaneamento_menos4,
             'horaatracacao': '00:00:01'})
        db['CARGA.AtracDesatracEscala'].insert(
            {'escala': 22, 'dataatracacao': data_escaneamento_menos2,
             'horaatracacao': '00:00:01'})
        # Exportação
        db['CARGA.AtracDesatracEscala'].insert(
            {'escala': 21, 'dataatracacao': data_escaneamento_mais8,
             'horaatracacao': '00:00:01'})
        create_indexes(db)
        carga.create_indexes(db)

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
        db['CARGA.NCM'].drop()

    def test_indexes(self):
        create_indexes(self.db)
        xmli.create_indexes(self.db)
        carga.create_indexes(self.db)

    def test_count(self):
        assert gridfs_count(self.db) == 8

    def test_summary(self):
        # Gravar dados do CARGA no fs.files
        carga.dados_carga_grava_fsfiles(
            self.db,
            data_inicio=self.data_escaneamento - timedelta(days=3))
        fs = GridFS(self.db)
        registro = self.db['fs.files'].find_one(
            {'metadata.numeroinformado': 'cheio'})
        _id = registro['_id']
        grid_data = fs.get(_id)
        text = dict_to_text(summary(grid_data=grid_data))
        textc = dict_to_text(carga.summary(registro=registro))
        html = dict_to_html(summary(registro=registro))
        htmlc = dict_to_html(carga.summary(grid_data=grid_data))
        assert text is not None
        assert 'cheio' in text
        assert textc is not None
        assert 'cheio' in textc
        assert html is not None
        assert 'cheio' in html
        assert htmlc is not None
        assert 'cheio' in htmlc

    def test_stats(self):
        atualiza_stats(self.db)
        stats = stats_resumo_imagens(self.db)
        assert stats is not None
        assert stats['Total de imagens'] == 7
        assert stats['Imagens com info do Carga'] == 0
        assert stats['Imagens com info do XML'] == 0
        assert stats['recinto']['A'] == 5
        assert stats['recinto']['B'] == 1
        assert stats['recinto']['C'] == 1
        recinto = stats['recinto']
        plot = plot_pie_plotly(list(recinto.values()), list(recinto.keys()))
        assert plot is not None
        assert isinstance(plot, str)
        stats = stats['recinto_mes'].get('A')
        plot = plot_bar_plotly(list(stats.values()), list(stats.keys()))
        assert plot is not None
        assert isinstance(plot, str)

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

    def test_busca_cheioe(self):
        # O caso de exportação é um caso especial,
        # pois ocorrem dois escaneamentos em poucos dias.
        # Neste teste, um contêiner chega em manifesto de vazio
        # e é escaneado.
        # Dois dias depois, é escaneado novamente
        # Oito dias depois, sai como exportação
        # Portanto é preciso primeiro achar o vazio e GRAVAR
        vazio = carga.busca_info_container(
            self.db, 'cheioe', self.data_escaneamento)
        assert vazio != {}
        assert vazio['vazio'] is True
        assert vazio['atracacao']['escala'] == 22
        assert vazio['container'][0]['manifesto'] == 22
        self.db['fs.files'].update(
            {'metadata.numeroinformado': 'cheioe',
             'metadata.dataescaneamento': self.data_escaneamento},
            {'$set': {'metadata.carga': vazio}}
        )
        cheioe = carga.busca_info_container(
            self.db, 'cheioe', self.data_escaneamento_cheioe)
        print('CHEIO EXP', cheioe)
        assert cheioe != {}
        assert cheioe['vazio'] is False
        assert cheioe['atracacao']['escala'] == 21
        assert cheioe['container'][0]['conhecimento'] == 21

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
        assert processados == 5
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
        assert semxml == 7
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
