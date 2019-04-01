from collections import defaultdict
from datetime import timedelta

from ajna_commons.flask.log import logger

from virasana.integracao.carga import create_indexes
from virasana.integracao.carga2 import carga_faltantes, mongo_find_in

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


def monta_mongo_dict(db, dict_conhecimentos_containeres):
    containers_set = set(dict_conhecimentos_containeres.keys())
    conhecimentos_set = set()
    for lista in dict_conhecimentos_containeres.values():
        for item in lista:
            conhecimentos_set.add(item)

    containers = mongo_find_in(db, 'CARGA.Container', ['conhecimento', 'container'],
                               [conhecimentos_set, containers_set], 'container')

    item_set = set()
    for container in containers.values():
        item_set.add(container['item'])
    conhecimentos = mongo_find_in(db, 'CARGA.Conhecimento', ['conhecimento'],
                                  [conhecimentos_set], 'conhecimento')
    ncms = mongo_find_in(db, 'CARGA.NCM', ['conhecimento', 'item'],
                         [conhecimentos_set, item_set], 'conhecimento')
    manifestos_conhecimentos = mongo_find_in(db, 'CARGA.ManifestoConhecimento', ['conhecimento'],
                                             [conhecimentos_set], 'conhecimento')
    manifestos_set = set()
    for conhecimento, linha in manifestos_conhecimentos.items():
        manifestos_set.add(linha['manifesto'])
    manifestos = mongo_find_in(db, 'CARGA.Manifesto', ['manifesto'], [manifestos_set], 'manifesto')
    manifestos_escala = mongo_find_in(db, 'CARGA.EscalaManifesto', ['manifesto'], [manifestos_set], 'manifesto')
    escalas_set = set([value['escala'] for value in manifestos_escala.values()])
    escalas = mongo_find_in(db, 'CARGA.Escala', ['escala'], [escalas_set], 'escala')
    atracacoes = mongo_find_in(db, 'CARGA.AtracDesatracEscala', ['escala'], [escalas_set], 'escala')
    mongo_dict = {}
    for container, values in containers.items():
        ldict = {'vazio': False}
        ldict['container'] = [values]
        ldict['conhecimento'] = list(conhecimentos.values())
        ldict['manifesto'] = list(manifestos.values())
        ldict['escala'] = list(escalas.values())
        ldict['ncm'] = list(ncms.values())
        ldict['atracacao'] = list(atracacoes.values())
        mongo_dict[container] = ldict

    return mongo_dict


def importacao_grava_fsfiles(db, data_inicio, data_fim):
    conhecimento_grava_fsfiles(db, data_inicio, data_fim, True)


def exportacao_grava_fsfiles(db, data_inicio, data_fim):
    conhecimento_grava_fsfiles(db, data_inicio, data_fim, False)


def conhecimento_grava_fsfiles(db, data_inicio, data_fim, importacao=True):
    if importacao:
        campo = 'escala'
        get_cursor = get_cursor_ceimportacao_mongo
    else:
        campo = 'conhecimento'
        get_cursor = get_cursor_ceexportacao_mongo

    dict_faltantes = carga_faltantes(db, data_inicio, data_fim, campo)
    total_fsfiles = len(dict_faltantes.keys())
    logger.info('Total de contêineres sem %s de %s a %s: %s' %
                (campo, data_inicio, data_fim, total_fsfiles))

    dict_conhecimentos = conhecimentos_periodo(db, data_inicio, data_fim,
                                               get_cursor)
    dict_conhecimentos_containeres = conhecimentos_containers_faltantes(
        dict_faltantes, dict_conhecimentos)

    total_conhecimentos = len(dict_conhecimentos_containeres)
    logger.info('Total de contêineres com conhecimento de %s a %s: %s' %
                (data_inicio, data_fim, total_conhecimentos))

    dados_carga = monta_mongo_dict(db, dict_conhecimentos_containeres)
    # dados_carga = {}
    for container, carga in dados_carga.items():
        _id = dict_faltantes[container]
        print(_id)
        db['fs.files'].update_one(
            {'_id': _id},
            {'$set': {'metadata.carga': carga}}
        )
    logger.info(
        'Resultado conhecimento_grava_fsfiles '
        'Pesquisados %s. '
        'Encontrados %s .'
        % (total_fsfiles, len(dados_carga))
    )


def compara_dicts(dict1, dict2):
    for key, value in dict1.items():
        value_fsfiles = dict2.get(key)
        if value_fsfiles is None:
            print('%s não existe' % key)
            continue
        if type(value) != type(value_fsfiles):
            print('Tipos diferentes: novo %s antigo %s. (%s) ' %
                  (type(value), type(value_fsfiles), key))
        if isinstance(value, list) and isinstance(value_fsfiles, list):
            for subvalue1, subvalue2 in zip(value, value_fsfiles):
                if isinstance(value, dict) and isinstance(value_fsfiles, dict):
                    compara_dicts(value, value_fsfiles)
            continue
        if isinstance(value, dict) and isinstance(value_fsfiles, dict):
            compara_dicts(value, value_fsfiles)
            continue
        if value_fsfiles != value:
            print('Valores diferentes: %s' % key)
            print('mongo_dict', value)
            print('fs.files', value_fsfiles)


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]

    print('Criando índices para CARGA')
    create_indexes(db)

    # Testa mongo_dict
    row = db.fs.files.find_one({'metadata.contentType': 'image/jpeg',
                                'metadata.carga.vazio': False},
                               {'metadata.carga': 1})

    container = row['metadata']['carga']['container'][0]['container']
    conhecimentos = [item['conhecimento']
                     for item in row['metadata']['carga']['conhecimento']]
    dict_conhecimentos_containeres = {
        container: conhecimentos
    }
    dict_faltantes = {
        container: row['_id']
    }
    dados_carga = monta_mongo_dict(db,
                                   dict_conhecimentos_containeres)

    dados_fsfiles = row['metadata']['carga']
    dados_carga = dados_carga[container]
    from pprint import pprint

    pprint(dados_carga)
    pprint(dados_fsfiles)
    compara_dicts(dados_carga, dados_fsfiles)
