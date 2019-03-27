"""Funções para leitura e tratamento dos dados de pesagem e gate dos recintos.
"""
import csv
import os
from collections import defaultdict
from datetime import date, datetime, timedelta

import requests
from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import sanitizar, mongo_sanitizar

FALTANTES = {'metadata.contentType': 'image/jpeg',
             'metadata.pesagens': {'$exists': False, '$eq': None}}

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

FIELDS = ()

# Fields to be converted to ISODate
DATE_FIELDS = ('Date', 'UpdateDateTime', 'LastStateDateTime',
               'SCANTIME', 'ScanTime')

DATA = 'metadata.xml.date'

CHAVES_RECINTO = [
    'metadata.pesagens.placacavalo',
    'metadata.pesagens.saida',
]


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração."""
    db['PesagensDTE'].create_index('codigoconteinerentrada')
    db['PesagensDTE'].create_index('codigoconteinesaida')
    db['PesagensDTE'].create_index('datahoraentradaiso')
    db['PesagensDTE'].create_index('datahorasaidaiso')
    db['PesagensDTE'].create_index('pesoentradafloat')
    db['PesagensDTE'].create_index('pesosaidafloat')
    db['PesagensDTE'].create_index('codigorecinto')
    db['fs.files'].create_index('metadata.pesagens.tipo')
    db['fs.files'].create_index('metadata.pesagens.entrada')
    db['fs.files'].create_index('metadata.pesagens.saida')
    db['fs.files'].create_index('metadata.pesagens.peso')
    db['fs.files'].create_index('metadata.pesagens.placacavalo')


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
    logger.debug('get_pesagens_dte ' + r.url)
    try:
        lista_pesagens = r.json()['JUP_WS']['Pesagens']['Lista_Pesagens']
    except:
        logger.error(r, r.text)
    return lista_pesagens


def get_pesagens_dte_recintos(recintos_list, datainicial, datafinal):
    token = get_token_dte()
    pesagens_recintos = defaultdict(list)
    for linha in recintos_list[1:]:
        recinto = linha[0]
        pesagens_recinto = get_pesagens_dte(datainicial, datafinal,
                                            recinto, token)
        if pesagens_recinto and len(pesagens_recinto) > 0:
            logger.info('%s: %s pesagens baixadas do recinto %s' %
                        (datainicial, len(pesagens_recinto), recinto))
            pesagens_recintos[recinto].extend(pesagens_recinto)
    return pesagens_recintos


def trata_registro_pesagem_dte(registro):
    new_dict = {}
    for key, value in registro.items():
        key = sanitizar(key, mongo_sanitizar)
        if value is not None:
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


def insert_pesagens_dte(db, pesagens_recintos):
    qtde = 0
    for recinto, pesagens in pesagens_recintos.items():
        for pesagem in pesagens:
            if pesagem['CodigoConteinerEntrada'] or pesagem['CodigoConteinerSaida']:
                pesagem_insert_mongo = {'codigo_recinto': recinto}
                pesagem_insert_mongo.update(trata_registro_pesagem_dte(pesagem))
                db['PesagensDTE'].insert_one(pesagem_insert_mongo)
                qtde += 1
    return qtde


def adquire_pesagens(db, datainicial, datafinal):
    ldata = datainicial
    # Trata somente um dia por vez, para não sobrecarregar DT-E
    while ldata <= datafinal:
        tem_passagem_na_data = db['PesagensDTE'].find_one(
            {'datahoraentradaiso': {'$gt': ldata,
                                    '$lt': ldata + timedelta(days=1)}})
        if tem_passagem_na_data:
            logger.info('adquire_pesagens dia %s abortado'
                        ' por já existirem registros' % ldata)
        else:
            pesagens = get_pesagens_dte_recintos(recintos_list, ldata, ldata)
            insert_ok = insert_pesagens_dte(db, pesagens)
        ldata = ldata + timedelta(days=1)


def compara_pesagens_imagens(fs_cursor, pesagens_cursor, campo_comparacao):
    ind = 0
    linhas_ainserir = []
    if fs_cursor and len(fs_cursor) > 0:
        fs_row = fs_cursor[ind]
        for pesagem in pesagens_cursor:
            while fs_row['metadata']['numeroinformado'].lower() < pesagem[campo_comparacao]:
                ind += 1
                if ind >= len(fs_cursor):
                    break
                fs_row = fs_cursor[ind]
            if fs_row['metadata']['numeroinformado'].lower() == pesagem[campo_comparacao]:
                linhas_ainserir.append((fs_row['_id'], pesagem))
    # Conferência do algoritmo
    # containers_imagens = [row['metadata']['numeroinformado'].lower() for row in fs_cursor]
    # containers_pesagens = [row[campo_comparacao] for row in pesagens_cursor]
    # containers_comuns = set(containers_imagens) & set(containers_pesagens)
    # print(len(containers_comuns))
    return linhas_ainserir


def inserepesagens_fsfiles(db, pesagens: list, tipo: str):
    cont = 0
    for linha in pesagens:
        _id = linha[0]
        dte = linha[1]
        registro = db.fs.files.find_one(
            {'_id': _id},
            ['metadata.pesagens']
        )
        pesagens = registro['metadata'].get('pesagens', [])
        if pesagens is None or not isinstance(pesagens, list):
            pesagens = []
        pesagem = {}
        pesagem['recinto'] = dte['recinto']
        pesagem['tipo'] = tipo
        pesagem['entrada'] = dte['datahoraentradaiso']
        pesagem['saida'] = dte.get('datahorasaidaiso', None)
        pesagem['placacavalo'] = dte['placacavalo']
        pesagem['placacarreta'] = dte['placacarreta']
        pesagem['pesoentrada'] = dte['pesoentradafloat']
        pesagem['pesosaida'] = dte['pesosaidafloat']
        # TODO: Será preciso procurar tara quando não informada
        pesagem['peso'] = abs(pesagem['pesoentrada'] - pesagem['pesosaida'])
        pesagem['carregadoentrada'] = dte['veiculocarregadoentradabool']
        pesagem['carregadosaida'] = dte['veiculocarregadosaidabool']
        if pesagem not in pesagens:
            cont += 1
            pesagens.append(pesagem)
        db.fs.files.update_one(
            {'_id': _id},
            {'$set': {'metadata.pesagens': pesagens}}
        )
    return cont


def pesagens_grava_fsfiles(db, data_inicio, data_fim):
    """Busca por registros no GridFS sem info da Pesagem

    Busca por registros no fs.files (GridFS - imagens) que não tenham metadata
    importada da pesagem.

    Args:
        db: connection to mongo with database setted
        batch_size: número de registros a consultar/atualizar por chamada
        data_inicio: filtra por data de escaneamento maior que a informada

    Returns:
        Número de registros encontrados

    """
    filtro = {'metadata.contentType': 'image/jpeg'}
    #  {'metadata.pesagens': {'$exists': False},
    DELTA = 5
    data_fim = data_fim + timedelta(hours=1, minutes=59, seconds=59)  # Pega atá a última hora do dia
    filtro['metadata.dataescaneamento'] = {'$gte': data_inicio, '$lte': data_fim}
    projection = ['metadata.numeroinformado', 'metadata.dataescaneamento']
    total = db['fs.files'].count_documents(filtro)
    fs_cursor = list(
        db['fs.files'].find(filtro, projection=projection).sort('metadata.numeroinformado')
    )
    pesagens_cursor_entrada = list(
        db['PesagensDTE'].find(
            {'datahoraentradaiso': {'$gte': data_inicio - timedelta(days=DELTA),
                                    '$lte': data_fim + timedelta(days=DELTA)},
             'codigoconteinerentrada': {'$exists': True, '$ne': None, '$ne': ''}}
        ).sort('codigoconteinerentrada')
    )
    pesagens_cursor_saida = list(
        db['PesagensDTE'].find(
            {'datahorasaidaiso': {'$gt': data_inicio - timedelta(days=DELTA),
                                  '$lt': data_fim + timedelta(days=DELTA)},
             'codigoconteinersaida': {'$exists': True, '$ne': None, '$ne': ''}}
        ).sort('codigoconteinersaida')
    )
    acum = 0
    logger.info(
        'Processando pesagens para imagens de %s a %s. '
        'Pesquisando pesagens %s dias antes e depois. '
        'Imagens encontradas: %s  Pesagens encontradas %s(entrada) %s(saída).'
        % (data_inicio, data_fim, DELTA, len(fs_cursor),
           len(pesagens_cursor_entrada), len(pesagens_cursor_saida))
    )
    linhas_entrada = compara_pesagens_imagens(fs_cursor, pesagens_cursor_entrada, 'codigoconteinerentrada')
    linhas_saida = compara_pesagens_imagens(fs_cursor, pesagens_cursor_saida, 'codigoconteinersaida')
    # acum = len(linhas_entrada) + len(linhas_saida)
    logger.info(
        'Resultado pesagens_grava_fsfiles '
        'Pesquisados %s. '
        'Encontrados %s entrada %s saida.'
        % (total, len(linhas_entrada), len(linhas_saida))
    )
    acum = 0
    acum += inserepesagens_fsfiles(db, linhas_entrada, 'entrada')
    acum += inserepesagens_fsfiles(db, linhas_saida, 'saida')
    return acum


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para Pesagens')
    print(create_indexes(db))
    print('Adquirindo pesagens do dia anterior')
    start = datetime.combine(date.today(), datetime.min.time()) - timedelta(days=1)
    end = start
    print(adquire_pesagens(db, start, end))
    print('Integrando pesagens do dia')
    print('Atualizados %s registros de pesagens' %
          pesagens_grava_fsfiles(db, start, end))
