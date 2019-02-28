"""
Definição dos códigos que são rodados para integração.

Background tasks do sistema AJNA-virasana

Aqui ficam as rotinas que serão chamadas periodicamente, visando integrar as
diversas bases entre elas, criar campos calculados, fazer manutenção na base,
integrar as predições, etc.

Este arquivo pode ser chamado em um prompt de comando no Servidor ou
programado para rodar via crontab, conforme exempo em /periodic_updates.sh

"""
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError

import requests
from ajna_commons.flask.conf import (DATABASE,
                                     MONGODB_URI, VIRASANA_URL)
from ajna_commons.flask.log import logger
from pymongo import MongoClient

from virasana.integracao import atualiza_stats, \
    carga, get_service_password, xmli
from virasana.scripts.gera_indexes import gera_indexes
from virasana.scripts.predictionsupdate import predictions_update2


def get_token(session, url):
    """Faz um get na url e tenta encontrar o csrf_token na resposta."""
    response = session.get(url)
    csrf_token = response.text
    begin = csrf_token.find('csrf_token"') + 10
    end = csrf_token.find('username"') - 10
    csrf_token = csrf_token[begin: end]
    begin = csrf_token.find('value="') + 7
    end = csrf_token.find('/>')
    csrf_token = csrf_token[begin: end]
    return csrf_token


def login(username, senha, session=None):
    """
    Autentica usuário no Servidor PADMA.

    Se não existir Usuário virasana, cria um com senha randômica
    """
    if session is None:
        session = requests.Session()
    url = VIRASANA_URL + '/login'
    csrf_token = get_token(session, url)
    # print('token*********', csrf_token)
    r = session.post(url, data=dict(
        username=username,
        senha=senha,
        csrf_token=csrf_token))
    return r


def reload_indexes():
    headers = {}
    result = {'success': False}
    s = requests.Session()
    username, password = get_service_password()
    r = login(username, password, s)
    try:
        print(VIRASANA_URL + '/recarrega_imageindex')
        r = s.get(VIRASANA_URL + '/recarrega_imageindex', headers=headers)
        if r.status_code == 200:
            result = r.json()
        print(result)
    except JSONDecodeError as err:
        print('Erro em reload_index(JSON inválido) %s HTTP Code:%s ' %
              (err, r.status_code))
    return result


def periodic_updates(db, lote=2000):
    print('Iniciando atualizações...')
    doisdias = datetime.now() - timedelta(days=2)
    xmli.dados_xml_grava_fsfiles(db, lote, doisdias)
    carga.dados_carga_grava_fsfiles(db, lote, doisdias)
    atualiza_stats(db)
    carga.cria_campo_pesos_carga(db, lote)
    predictions_update2('ssd', 'bbox', lote, 4)
    predictions_update2('index', 'index', lote, 8)
    gera_indexes()
    print(reload_indexes())
    predictions_update2('vaziosvm', 'vazio', lote, 4)
    predictions_update2('peso', 'peso', lote, 16)


if __name__ == '__main__':
    os.environ['DEBUG'] = '1'
    logger.setLevel(logging.DEBUG)
    with MongoClient(host=MONGODB_URI) as conn:
        db = conn[DATABASE]
        daemonize = '--daemon' in sys.argv
        periodic_updates(db)
        s0 = time.time()
        while daemonize:
            time.sleep(2)
            if time.time() - s0 > (30 * 60):
                periodic_updates(db)
                s0 = time.time()
