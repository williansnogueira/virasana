"""
Definição dos códigos que serão rodados pelo Celery.

Background tasks do sistema AJNA-virasana
Gerenciados por celery_.sh
Aqui ficam as rotinas que serão chamadas periodicamente.

"""
import sys
import time
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError

import requests
from ajna_commons.flask.conf import (DATABASE,
                                     MONGODB_URI, VIRASANA_URL)
from pymongo import MongoClient

from virasana.integracao import carga, get_service_password, xmli
from virasana.scripts.gera_indexes import gera_indexes
from virasana.scripts.predictionsupdate import predictions_update2
from virasana.workers.tasks import celery, processa_bson, processa_carga, \
    processa_predictions


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """Agenda tarefas que serão executadas com frequência fixa.

    Os tempos são em segundos
    """
    # Tempos "quebrados" para evitar simultaneidade
    # processa_xml será chamado por processa_bson
    sender.add_periodic_task(30 * 60.0, processa_bson.s())  # 30 min
    # sender.add_periodic_task(11 * 60.1, processa_xml.s())  # 11 min
    sender.add_periodic_task(61 * 60.0, processa_predictions.s())  # 61 min
    sender.add_periodic_task(6 * 3603.0, processa_carga.s())  # 6h
    # sender.add_periodic_task(12 * 3600.00, processa_stats.s())  # 12h


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
    print('token*********', csrf_token)
    r = session.post(url, data=dict(
        username=username,
        senha=senha,
        csrf_token=csrf_token))
    return r


def reload_indexes():
    headers = {}
    result = {'predictions': [], 'success': False}
    s = requests.Session()
    username, password = get_service_password()
    r = login(username, password, s)
    try:
        r = s.get(VIRASANA_URL + '/recarrega_imageindex', headers=headers)
        if r.status_code == 200:
            result = r.json()
        print(r.json())
    except JSONDecodeError as err:
        print('Erro em reload_index(JSON inválido) %s HTTP Code:%s ' %
              (err, r.status_code))
    return result


def periodic_updates(db):
    print('Iniciando atualizações...')
    doisdias = datetime.now() - timedelta(days=2)
    xmli.dados_xml_grava_fsfiles(db, 3000, doisdias)
    carga.dados_carga_grava_fsfiles(db, 3000, doisdias)
    carga.cria_campo_pesos_carga(db, 1000)
    # predictions_update2('ssd', 'bbox', 3000, 4)
    predictions_update2('index', 'index', 3000, 4)
    gera_indexes()
    predictions_update2('vaziosvm', 'vazio', 3000, 4)
    predictions_update2('peso', 'peso', 3000, 4)


if __name__ == '__main__':
    with MongoClient(host=MONGODB_URI) as conn:
        db = conn[DATABASE]
        daemonize = '--daemon' in sys.argv
        periodic_updates(db)
        s0 = time.time()
        while daemonize:
            time.sleep(2)
            if time.time() - s0 > (30 * 60):
                periodic_updates()
                s0 = time.time()
