"""
Definição dos códigos que serão rodados pelo Celery.

Background tasks do sistema AJNA-virasana
Gerenciados por celery.sh
Aqui ficam as que rodam tarefas custosas/demoradas em background.

"""
# Código dos celery tasks
import json
from datetime import datetime, timedelta
from base64 import decodebytes

import gridfs
from celery import Celery, states
from pymongo import MongoClient

from ajna_commons.flask.conf import (BACKEND, BROKER, BSON_REDIS, DATABASE,
                                     MONGODB_URI, redisdb)
from ajna_commons.flask.log import logger
from ajna_commons.models.bsonimage import BsonImageList
from virasana.integracao import atualiza_stats, carga, xmli
from virasana.scripts.predictionsupdate import predictions_update
from virasana.workers.dir_monitor import despacha_dir

celery = Celery(__name__, broker=BROKER,
                backend=BACKEND)


# Tasks que respondem a ações da VIEW
def trata_bson(bson_file: str, db: MongoClient) -> list:
    """Recebe o nome de um arquivo bson e o insere no MongoDB."""
    # .get_default_database()
    fs = gridfs.GridFS(db)
    bsonimagelist = BsonImageList.fromfile(abson=bson_file)
    files_ids = bsonimagelist.tomongo(fs)
    return files_ids


@celery.task(bind=True)
def raspa_dir(self):
    """Carrega arquivos do REDIS para mongoDB.

    Tarefa de background que recebe arquivos da view via REDIS
    e os carrega para o MongoDB.
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


@celery.task
def processa_bson():
    """Chama função do módulo dir_monitor.

    Para permitir o upload de BSON do AVATAR através da simples
    colocação do arquivo em um diretório.
    Neste módulo pode ser configurado o endereço de um diretório
    e o endereço do virasana. A função a seguir varre o diretório e,
    havendo arquivos, envia por request POST para o URL do virasana.
    Se obtiver sucesso, exclui o arquivo enviado do diretório.


    """
    print('TESTE')
    logger.info('Varrendo diretório...')
    dir, erros, excecoes = despacha_dir()
    logger.info('Varreu diretório %s. Erros %s' % (dir, erros))
    logger.info('Atualizando metadata XML...')
    num2, num5 = processa_xml()
    logger.info('Metadata XML atualizado. '
                '%s novos nos últimos cinco dias, '
                '%s novos nos últimos dois anos' %
                (num2, num5))


@celery.task
def processa_xml():
    """Verifica se há arquivos XML a carregar no GridFS.

    Verifica se há novas imagens no Banco de Dados que ainda estão
    sem a integração com XML. Havendo, grava dados XML disponíveis,
    se encontrados, no GridFS
    """
    with MongoClient(host=MONGODB_URI) as conn:
        db = conn[DATABASE]
        cincodias = datetime.now() - timedelta(days=5)
        num5 = xmli.dados_xml_grava_fsfiles(db, 1000, cincodias)
        # Olhar o passado tbm...
        doisanos = datetime.now() - timedelta(days=730)
        num2 = xmli.dados_xml_grava_fsfiles(db, 1000, doisanos)
    return num5, num2


@celery.task
def processa_carga():
    """Verifica se há dados do Sistema CARGA a carregar no GridFS.

    Verifica se há novas imagens no Banco de Dados que ainda estão
    sem a integração com o sistema CARGA. Havendo, grava dados disponíveis,
    se encontrados, no GridFS
    """
    with MongoClient(host=MONGODB_URI) as conn:
        db = conn[DATABASE]
        doisdias = datetime.now() - timedelta(days=2)
        carga.dados_carga_grava_fsfiles(db, 5000, doisdias)


@celery.task
def processa_stats():
    """Chama função do módulo integracao.

    O módulo integração pode definir uma função atualiza_stats
    Sua função é criar coleções de estatísticas sobre o Banco de Dados
    que seriam custosas de produzir on-line.


    """
    with MongoClient(host=MONGODB_URI) as conn:
        db = conn[DATABASE]
        atualiza_stats(db)


@celery.task
def processa_predictions():
    """Roda modelos de aprendizado de máquina disponíveis.

    Roda modelos e adiciona resultado no metadata.

    Serve também como documentação dos modelos atualmente recomendados
    no pipeline padrão.

    """
    predictions_update('ssd', 'bbox', 1000, 4)
    predictions_update('index', 'index', 1000, 4)
    predictions_update('vaziosvm', 'vazio', 1000, 4)
    predictions_update('peso', 'peso', 1000, 4)
