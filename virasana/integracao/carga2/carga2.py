import datetime
import time

from ajna_commons.flask.log import logger

from virasana.integracao.carga2.conhecimento import conhecimento_grava_fsfiles
from virasana.integracao.carga2.manifesto import manifestos_periodo, \
    manifestos_unicos_containers, get_cursor_vazios_mongo, \
    monta_mongo_dict, manifesto_grava_fsfiles

FALTANTES = {'metadata.contentType': 'image/jpeg',
             'metadata.carga.atracacao.escala': None}

DELTA_IMPORTACAO = -5
DELTA_EXPORTACAO = +10


def carga_grava_fsfiles(db, data_inicio, data_fim):
    """Busca por registros no GridFS sem info da Pesagem

    Busca por registros no fs.files (GridFS - imagens) que não tenham metadata
    importada da pesagem.

    Args:
        db: connection to mongo with database setted

        data_inicio: filtra por data de escaneamento maior que a informada

    Returns:
        Número de registros encontrados

    """
    manifesto_grava_fsfiles(db, data_inicio, data_fim)
    conhecimento_grava_fsfiles(db, data_inicio, data_fim)
    conhecimento_grava_fsfiles(db, data_inicio, data_fim, importacao=False)


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    start = datetime.datetime(2019, 3, 20)
    end = datetime.datetime(2019, 3, 24)
    ldata = start
    while ldata <= end:
        s0 = time.time()
        logger.info('Integrando Manifestos dia %s  a %s...' % (ldata, ldata))
        manifesto_grava_fsfiles(db, ldata, ldata)
        s1 = time.time()
        logger.info('Manifestos atualizados em %s segundos.' % (s1 - s0))

        s0 = time.time()
        logger.info('Integrando Importação dia %s  a %s...' % (ldata, ldata))
        conhecimento_grava_fsfiles(db, ldata, ldata)
        s1 = time.time()
        logger.info('Conhecimentos atualizados em %s segundos.' % (s1 - s0))

        s0 = time.time()
        logger.info('Integrando Exportação dia %s  a %s...' % (ldata, ldata))
        conhecimento_grava_fsfiles(db, ldata, ldata, importacao=False)
        s1 = time.time()
        logger.info('Conhecimentos atualizados em %s segundos.' % (s1 - s0))
        ldata = ldata + datetime.timedelta(days=1)


