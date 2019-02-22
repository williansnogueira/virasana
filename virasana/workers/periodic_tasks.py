"""
Definição dos códigos que serão rodados pelo Celery.

Background tasks do sistema AJNA-virasana
Gerenciados por celery_.sh
Aqui ficam as rotinas que serão chamadas periodicamente.

"""

import time
from datetime import datetime, timedelta
from virasana.workers.tasks import celery, processa_bson, processa_carga,\
    processa_predictions
from ajna_commons.flask.conf import (BACKEND, BROKER, BSON_REDIS, DATABASE,
                                     MONGODB_URI, redisdb)
from pymongo import MongoClient

from virasana.integracao import xmli
from virasana.scripts.predictionsupdate import predictions_update


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


if __name__=='__main__':
    with MongoClient(host=MONGODB_URI) as conn:
        db = conn[DATABASE]
        s0 = time.time() - (15*60)
        while True:
            time.sleep(2)
            if time.time() - s0 > (15*60):
                print("Iniciando atualizações...")
                doisdias = datetime.now() - timedelta(days=2)
                num5 = xmli.dados_xml_grava_fsfiles(db, 2000, doisdias)
                predictions_update('ssd', 'bbox', 500, 4)
                predictions_update('index', 'index', 500, 4)
                predictions_update('vaziosvm', 'vazio', 500, 4)
                predictions_update('peso', 'peso', 500, 4)
                s0 = time.time()

