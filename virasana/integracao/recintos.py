"""Funções para leitura e tratamento dos dados de pesagem e gate dos recintos.
"""
import csv
import os
from collections import defaultdict
from datetime import datetime, timedelta

import requests
from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import sanitizar, unicode_sanitizar, mongo_sanitizar

DTE_USERNAME = os.environ.get('DTE_USERNAME')
DTE_PASSWORD = os.environ.get('DTE_PASSWORD')

if DTE_PASSWORD is None:
    dte_file = os.path.join(os.path.dirname(__file__), 'dte.info')
    with open(dte_file) as dte_info:
        linha = dte_info.readline()
    DTE_USERNAME = linha.split(',')[0]
    DTE_PASSWORD = linha.split(',')[1]

try:
    recintos_file = os.path.join(os.path.dirname(__file__), 'recintos.csv')
    with open(recintos_file, encoding='utf-8') as csv_in:
        reader = csv.reader(csv_in)
        recintos_list = [row for row in reader]
except FileNotFoundError:
    recintos_list = []

DTE_URL = 'https://www.janelaunicaportuaria.org.br/ws_homologacao/sepes/api/Pesagem'

FALTANTES = {'metadata.recinto': {'$exists': False},
             'metadata.contentType': 'image/jpeg'}

FIELDS = ()

# Fields to be converted to ISODate
DATE_FIELDS = ('Date', 'UpdateDateTime', 'LastStateDateTime',
               'SCANTIME', 'ScanTime')

DATA = 'metadata.xml.date'

CHAVES_RECINTO = [
    'metadata.recinto.container',
    'metadata.xml.alerta',
]


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração."""
    db['fs.files'].create_index('metadata.xml.container')
    db['fs.files'].create_index('metadata.xml.alerta')


def get_token_dte(username=DTE_USERNAME, password=DTE_PASSWORD):
    data = {'username': username, 'password': password, 'grant_type': 'password'}
    r = requests.post(DTE_URL + '/token', data=data)
    token = r.json().get('access_token')
    return token


def get_pesagens_dte(datainicial, datafinal, recinto, token):
    payload = {'data_inicio': datetime.strftime(datainicial, '%Y-%m-%d'),
               'data_fim': datetime.strftime(datafinal, '%Y-%m-%d'),
               'cod_recinto': recinto}
    headers = {'Authorization': 'Bearer ' + token}
    r = requests.get(DTE_URL, headers=headers, params=payload)
    print(r.url)
    try:
        lista_pesagens = r.json()['JUP_WS']['Pesagens']['Lista_Pesagens']
    except:
        print(r, r.text)
    return lista_pesagens


def get_pesagens_dte_recintos(recintos_list, datainicial, datafinal):
    token = get_token_dte()
    pesagens_recintos = defaultdict(list)
    for linha in recintos_list[1:]:
        recinto = linha[0]
        pesagens_recinto = get_pesagens_dte(datainicial, datafinal,
                                            recinto, token)
        if pesagens_recinto and len(pesagens_recinto) > 0:
            print(recinto, len(pesagens_recinto))
            pesagens_recintos[recinto].extend(pesagens_recinto)
    return pesagens_recintos


def trata_registro_pesagem_dte(registro):
    new_dict = {}
    for key, value in registro.items():
        key = sanitizar(key, mongo_sanitizar)
        value = sanitizar(value, mongo_sanitizar)
        new_dict[key] = value
    new_dict['datahoraentradaiso'] = datetime.strptime(new_dict['datahoraentrada'],
                                                       '%Y-%m-%d %H:%M:%S')
    datahorasaida = new_dict['datahorasaida']
    if datahorasaida:
        new_dict['datahorasaidaiso'] = datetime.strptime(new_dict['datahorasaida'],
                                                         '%Y-%m-%d %H:%M:%S')
    new_dict['pesoentradafloat'] = float(new_dict['pesoentrada'].replace(',', '.'))
    new_dict['pesosaidafloat'] = float(new_dict['pesosaida'].replace(',', '.'))
    new_dict['veiculocarregadosaidabool'] = new_dict['veiculocarregadosaida'] == "sim"
    new_dict['veiculocarregadoentradabool'] = new_dict['veiculocarregadoentrada'] == "sim"
    return (new_dict)


def insert_pesagens_dte(pesagens_recintos):
    qtde = 0
    for recinto, pesagens in pesagens_recintos.items():
        for pesagem in pesagens:
            if pesagem['CodigoConteinerEntrada'] or pesagem['CodigoConteinerSaida']:
                pesagem_insert_mongo = {'codigo_recinto': recinto}
                pesagem_insert_mongo.update(trata_registro_pesagem_dte(pesagem))
                db['PesagensDTE'].insert_one(pesagem_insert_mongo)
                qtde += 1
    return qtde


def adquire_pesagens(datainicial, datafinal):
    pesagens = get_pesagens_dte_recintos(recintos_list, datainicial, datafinal)
    return insert_pesagens_dte(pesagens)



def dados_xml_grava_fsfiles(db, batch_size=5000,
                            data_inicio=datetime(1900, 1, 1),
                            update=True):
    """Busca por registros no GridFS sem info da Pesagem

    Busca por registros no fs.files (GridFS - imagens) que não tenham metadata
    importada do arquivo XML. Itera estes registros, consultando a
    xml_todict para ver se retorna informações do XML. Encontrando
    estas informações, grava no campo metadata.xml do fs.files

    Args:
        db: connection to mongo with database setted

        batch_size: número de registros a consultar/atualizar por chamada

        data_inicio: filtra por data de escaneamento maior que a informada

        update: Caso seja setado como False, apenas faz consulta, sem
            atualizar metadata da collection fs.files

    Returns:
        Número de registros encontrados

    """
    total = db['fs.files'].count_documents(FALTANTES).limit(batch_size)
    file_cursor = db['fs.files'].find(FALTANTES).limit(batch_size)
    acum = 0
    for linha in file_cursor:
        acum += 1
    logger.info(' '.join([
        'Resultado dados_xml_grava_fsfiles',
        'Pesquisados', str(total),
        'Encontrados', str(acum)
    ]))
    return acum


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para Pesagens')
    create_indexes(db)
    start = end = datetime.now() - timedelta(days=1)
    adquire_pesagens(start, end)

