import time
from collections import OrderedDict
import datetime


from ajna_commons.flask.log import logger

from virasana.integracao.carga2.manifesto import manifestos_periodo, \
    manifestos_unicos_containers, get_cursor_vazios_mongo, \
    monta_mongo_dict

FALTANTES = {'metadata.contentType': 'image/jpeg',
             'metadata.carga.atracacao.escala': None}

DELTA_IMPORTACAO = -5
DELTA_EXPORTACAO = +10


def carga_faltantes(data_inicio, data_fim, campo):
    dict_faltantes = OrderedDict()
    filtro = FALTANTES
    data_fim = datetime.datetime.combine(data_fim, datetime.time.max)  # Pega atá a última hora do dia
    filtro['metadata.dataescaneamento'] = {'$gte': data_inicio, '$lte': data_fim}
    projection = ['metadata.numeroinformado', 'metadata.dataescaneamento']
    # print(filtro)
    fs_cursor = db['fs.files'].find(filtro, projection=projection).sort('metadata.numeroinformado')
    for linha in fs_cursor:
        numero = linha['metadata']['numeroinformado'].lower()
        dict_faltantes[numero] = linha['_id']
    return dict_faltantes


def carga_grava_fsfiles(db, data_inicio, data_fim, campo):
    """Busca por registros no GridFS sem info da Pesagem

    Busca por registros no fs.files (GridFS - imagens) que não tenham metadata
    importada da pesagem.

    Args:
        db: connection to mongo with database setted

        data_inicio: filtra por data de escaneamento maior que a informada

    Returns:
        Número de registros encontrados

    """
    dict_faltantes = carga_faltantes(data_inicio, data_fim, campo)
    total_fsfiles = len(dict_faltantes.keys())
    logger.info('Total de contâineres sem %s de %s a %s: %s' %
                (campo, data_inicio, data_fim, total_fsfiles))

    dict_manifestos = manifestos_periodo(db, data_inicio, data_fim,
                                         get_cursor_vazios_mongo)
    dict_manifestos_containeres = manifestos_unicos_containers(
        dict_faltantes, dict_manifestos)
    total_manifestos = len(dict_manifestos_containeres.keys())
    logger.info('Total de manifestos de %s a %s: %s' %
                (data_inicio, data_fim, total_manifestos))

    dados_carga = monta_mongo_dict(db,
                                  dict_manifestos_containeres,
                                  dict_faltantes)
    # print(mongo_dict)
    for container, carga in dados_carga.items():
        _id = dict_faltantes[container]
        print(_id)
        db['fs.files'].update_one(
            {'_id': _id},
            {'$set': {'metadata.carga': carga}}
        )

    logger.info(
        'Resultado pesagens_grava_fsfiles '
        'Pesquisados %s. '
        'Encontrados %s .'
        % (total_fsfiles, len(dados_carga))
    )


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    s0 = time.time()
    start = datetime.datetime(2017, 7, 5)
    end = datetime.datetime(2017, 7, 15)
    # print('Integrando Manifestos dia %s  a %s...' % (start, end))
    # carga_grava_fsfiles(db, start, end, 'manifesto')

    ldata = start
    while ldata <= end:
        print('Integrando Manifestos dia %s ...' % ldata)
        carga_grava_fsfiles(db, ldata, ldata, 'manifesto')
        ldata = ldata + datetime.timedelta(days=1)
    s1 = time.time()
    logger.info('Manifestos atualizados em %s segundos.' % (s1 - s0))
