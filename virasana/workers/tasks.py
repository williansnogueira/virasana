"""
Definição dos códigos que serão rodados pelo Celery.

Background tasks do sistema AJNA-virasana
Gerenciados por celery.sh
Aqui ficam as rotinas que serão chamadas periodicamente e
aquelas que rodam tarefas custosas/demoradas em background.

"""
# Código dos celery tasks
import json
from base64 import decodebytes
# from datetime import datetime

import gridfs
from celery import Celery, states
from pymongo import MongoClient

from ajna_commons.flask.conf import (BACKEND, BROKER, BSON_REDIS, DATABASE,
                                     MONGODB_URI, redisdb)
from ajna_commons.models.bsonimage import BsonImageList
from virasana.integracao import carga, xml

from .dir_monitor import despacha_dir

celery = Celery(__name__, broker=BROKER,
                backend=BACKEND)


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
<<<<<<< HEAD
    """Configura as tarefas periódicas."""
    sender.add_periodic_task(15*60.0, processa_carga.s())
    sender.add_periodic_task(13*60.0, processa_xml.s())
=======
    sender.add_periodic_task(15 * 60.0, processa_carga.s())
    sender.add_periodic_task(13 * 60.0, processa_xml.s())
    sender.add_periodic_task(5 * 60.0, processa_bson.s())
>>>>>>> 89608b622b4d7c48e0c05628b089059255a8babe


@celery.task
def processa_xml():
    """Verifica se há arquivos XML a carregar no GridFS.

    Verifica se há novas imagens no Banco de Dados que ainda estão
    sem a integração com XML. Havendo, grava dados XML disponíveis,
    se encontrados, no GridFS
    """
    with MongoClient(host=MONGODB_URI) as conn:
        db = conn[DATABASE]
        xml.dados_xml_grava_fsfiles(db)


@celery.task
def processa_carga():
    """Verifica se há dados do Sistema CARGA a carregar no GridFS.

    Verifica se há novas imagens no Banco de Dados que ainda estão
    sem a integração com o sistema CARGA. Havendo, grava dados disponíveis,
    se encontrados, no GridFS
    """
    with MongoClient(host=MONGODB_URI) as conn:
        db = conn[DATABASE]
        carga.dados_carga_grava_fsfiles(db)


@celery.task
def processa_bson():
    """Chama função do módulo dir_monitor.

    Neste módulo pode ser configurado o endereço de um diretório
    e o endereço do virasana. A função a seguir varre o diretório e,
    havendo arquivos, envia por request POST para o URL do virasana.
    Se obtiver sucesso, exclui o arquivo enviado do diretório
    """
    despacha_dir()


@celery.task(bind=True)
def raspa_dir(self):
    """De base em arquivos para mongoDB.

    Background task that go to redis DB of incoming files
    AND load then to mongodb
    """
    self.update_state(state=states.STARTED,
                      meta={'current': '',
                            'status': 'Iniciando'})
    q = redisdb.lpop(BSON_REDIS)
    q = json.loads(q.decode('utf-8'))
    self.update_state(meta={'current': q.get('filename'),
                            'status': 'Processando arquivo'})
    file = bytes(q['bson'], encoding='utf-8')
    file = decodebytes(file)
    with MongoClient(host=MONGODB_URI) as conn:
        db = conn[DATABASE]
        trata_bson(file, db)
    return {'current': '',
            'status': 'Todos os arquivos processados'}


def trata_bson(bson_file: str, db: MongoClient) -> list:
    """Recebe o nome de um arquivo bson e o insere no MongoDB."""
    # .get_default_database()
    fs = gridfs.GridFS(db)
    bsonimagelist = BsonImageList.fromfile(abson=bson_file)
    files_ids = bsonimagelist.tomongo(fs)
    return files_ids
