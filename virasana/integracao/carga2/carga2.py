import time
from datetime import timedelta

import pymongo
from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from ajna_commons.flask.log import logger

from virasana.integracao import carga, create_indexes
from virasana.integracao.carga2 import Tipo
from virasana.integracao.carga2.conhecimento import \
    conhecimento_grava_fsfiles, exportacao_grava_fsfiles, \
    importacao_grava_fsfiles
from virasana.integracao.carga2.manifesto import manifesto_grava_fsfiles

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


def maior_data_atracacao(db, campo_carga, tipo_manifesto=None):
    campo_carga = 'metadata.carga.%s' % campo_carga
    filtro = {campo_carga: {'$exists': True}}
    if tipo_manifesto is not None:
        filtro['metadata.carga.manifesto.tipomanifesto'] = {'$eq': 'lce'}
    projection = {'metadata.carga.atracacao.dataatracacaoiso': 1, '_id': 0}
    atracacao = db.fs.files.find(
        filtro, projection
    ).sort([('metadata.carga.atracacao.dataatracacaoiso', pymongo.ASCENDING)]
           ).limit(1)[0]
    atracacao = atracacao.get('metadata').get('carga').get('atracacao')
    if isinstance(atracacao, list):
        atracacao = atracacao[0]
    return atracacao.get('dataatracacaoiso')


def update_ultima_dataatracacaoiso(db, tipo=Tipo.MANIFESTO, range=10):
    if tipo == Tipo.MANIFESTO:
        ultima_atracacao_manifesto = \
            maior_data_atracacao(db, 'manifesto.manifesto')
        # Começa manifestos três dias antes para pegar manifestos de exportação
        start = ultima_atracacao_manifesto - timedelta(days=3)
        grava = manifesto_grava_fsfiles
        msg = 'Manifestos'
    elif tipo == Tipo.IMPORTACAO:
        ultima_atracacao_impo = \
            maior_data_atracacao(db, 'conhecimento.conhecimento', 'lci')
        start = ultima_atracacao_impo - timedelta(days=1)
        grava = importacao_grava_fsfiles
        msg = 'Conhecimentos de importação'
    else:
        ultima_atracacao_expo = \
            maior_data_atracacao(db, 'conhecimento.conhecimento', 'lce')
        start = ultima_atracacao_expo - timedelta(days=DELTA_EXPORTACAO)
        grava = exportacao_grava_fsfiles
        msg = 'Conhecimentos de exportação'

    end = start + timedelta(days=range)
    ldata = start
    s0 = time.time()
    total = 0
    while ldata <= end:
        s1 = time.time()
        logger.info('Integrando %s dia %s...' % (msg, ldata))
        total += grava(db, ldata, ldata)
        s2 = time.time()
        logger.info('%s atualizados em %s segundos.' % (msg, (s2 - s1)))
        logger.info('Tempo decorrido: %s segundos.' % (s2 - s0))
        ldata = ldata + timedelta(days=1)
    logger.info('Total de %s %s atualizados em %s segundos.' %
                (total, msg, (s2 - s0)))
    logger.info('Tempo total decorrido: %s segundos.' % (s2 - s0))


def do_update_carga(db):
    print('Aguarde, criando/atualizando índices')
    create_indexes(db)
    carga.create_indexes(db)
    print('OK! Índices criados/atualizados.')
    for tipo in Tipo:
        update_ultima_dataatracacaoiso(db, tipo)


if __name__ == '__main__':  # pragma: no cover
    db = pymongo.MongoClient(host=MONGODB_URI)[DATABASE]
    do_update_carga(db)
