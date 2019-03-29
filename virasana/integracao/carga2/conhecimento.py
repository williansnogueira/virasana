from collections import defaultdict
from datetime import timedelta

from ajna_commons.flask.log import logger

from virasana.integracao.carga2 import carga_faltantes

DELTA_IMPORTACAO = 5
DELTA_EXPORTACAO = 10


def get_cursor_conhecimentos_mongo(db, start, end, deltaantes, deltadepois, tipos_manifesto):
    AGREGATE_CONHECIMENTO = [
        {'$match': {'dataatracacaoiso':
                        {'$gte': start - timedelta(days=deltaantes),
                         '$lte': end + timedelta(days=deltadepois)}}},
        {'$lookup': {
            'from': 'CARGA.EscalaManifesto',
            'localField': 'escala',
            'foreignField': 'escala',
            'as': 'lista_conhecimentos'
        }},
        {'$project': {'escala': 1, 'dataatracacaoiso': 1,
                      'lista_conhecimentos.manifesto': 1}},
        {'$lookup': {
            'from': 'CARGA.Manifesto',
            'localField': 'lista_conhecimentos.manifesto',
            'foreignField': 'manifesto',
            'as': 'lista_conhecimentos2'
        }},
        {'$match': {'lista_conhecimentos2.tipomanifesto':
                        {'$in': tipos_manifesto}}},
        {'$project': {'escala': 1, 'dataatracacaoiso': 1,
                      'lista_conhecimentos2.manifesto': 1}},
        {'$lookup': {
            'from': 'CARGA.ManifestoConhecimento',
            'localField': 'lista_conhecimentos2.manifesto',
            'foreignField': 'manifesto',
            'as': 'conhecimentos'
        }},
        {'$project': {'escala': 1, 'dataatracacaoiso': 1,
                      'conhecimentos.conhecimento': 1,
                      'conhecimentos.manifesto': 1}},
        {'$lookup': {
            'from': 'CARGA.Container',
            'localField': 'conhecimentos.conhecimento',
            'foreignField': 'conhecimento',
            'as': 'containers'
        }},
        {'$project': {'escala': 1, 'dataatracacaoiso': 1,
                      'conhecimentos.conhecimento': 1,
                      'conhecimentos.manifesto': 1,
                      'containers.conhecimento': 1,
                      'containers.container': 1
                      }}
    ]
    return db['CARGA.AtracDesatracEscala'].aggregate(AGREGATE_CONHECIMENTO)


def get_cursor_ceimportacao_mongo(db, start, end):
    return get_cursor_conhecimentos_mongo(db, start, end,
                                          DELTA_IMPORTACAO, 0, ['lci', 'bce'])


def get_cursor_ceexportacao_mongo(db, start, end):
    return get_cursor_conhecimentos_mongo(db, start, end, 1,
                                          DELTA_EXPORTACAO, ['lce'])


def conhecimentos_periodo(db, start, end, cursor_function):
    dict_conhecimentos = defaultdict(list)
    fs_cursor = cursor_function(db, start, end)
    for linha in fs_cursor:
        for conhecimento_container in linha['containers']:
            conhecimento = conhecimento_container['conhecimento']
            container = conhecimento_container['container']
            dict_conhecimentos[container].append(conhecimento)
    return dict_conhecimentos


def conhecimentos_containers_faltantes(dict_faltantes, dict_conhecimentos):
    dict_conhecimentos_filtrado = defaultdict(list)
    for numero, _id in dict_faltantes.items():
        conhecimentos = dict_conhecimentos.get(numero)
        if conhecimentos is not None:
            dict_conhecimentos_filtrado[numero] = conhecimentos
    return dict_conhecimentos_filtrado


def conhecimento_grava_fsfiles(db, data_inicio, data_fim, importacao=True):
    campo = 'escala'
    dict_faltantes = carga_faltantes(db, data_inicio, data_fim, campo)
    total_fsfiles = len(dict_faltantes.keys())
    logger.info('Total de contêineres sem %s de %s a %s: %s' %
                (campo, data_inicio, data_fim, total_fsfiles))

    if importacao:
        get_cursor = get_cursor_ceimportacao_mongo
    else:
        get_cursor = get_cursor_ceexportacao_mongo
    dict_conhecimentos = conhecimentos_periodo(db, data_inicio, data_fim,
                                               get_cursor)
    dict_conhecimentos_containeres = conhecimentos_containers_faltantes(
        dict_faltantes, dict_conhecimentos)
    total_conhecimentos = len(dict_conhecimentos_containeres.keys())
    logger.info('Total de conhecimentos para estes contêineres de %s a %s: %s' %
                (data_inicio, data_fim, total_conhecimentos))
