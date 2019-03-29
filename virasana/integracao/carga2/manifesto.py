from collections import defaultdict, OrderedDict
from datetime import timedelta

from ajna_commons.flask.log import logger

from virasana.integracao.carga2 import carga_faltantes

DELTA_VAZIO = 5


def get_cursor_vazios_mongo(db, start, end):
    AGREGATE_VAZIO = [
        {'$match': {'dataatracacaoiso':
                        {'$gte': start - timedelta(days=DELTA_VAZIO),
                         '$lte': end + timedelta(days=DELTA_VAZIO)}}},
        {'$lookup': {
            'from': 'CARGA.EscalaManifesto',
            'localField': 'escala',
            'foreignField': 'escala',
            'as': 'lista_manifestos'
        }},
        {'$project': {'escala': 1, 'dataatracacaoiso': 1,
                      'lista_manifestos.manifesto': 1}},
        {'$lookup': {
            'from': 'CARGA.ContainerVazio',
            'localField': 'lista_manifestos.manifesto',
            'foreignField': 'manifesto',
            'as': 'containers_vazios'
        }},
        {'$project': {'escala': 1, 'dataatracacaoiso': 1,
                      'containers_vazios.container': 1,
                      'containers_vazios.manifesto': 1}},
        {'$sort': {'containers_vazios.container': 1}}
    ]

    return db['CARGA.AtracDesatracEscala'].aggregate(AGREGATE_VAZIO)


def manifestos_periodo(db, start, end, cursor_function):
    dict_manifestos = dict()
    fs_cursor = cursor_function(db, start, end)
    for lista in fs_cursor:
        for linha in lista['containers_vazios']:
            manifesto = linha['manifesto']
            dict_manifestos[linha['container']] = manifesto
    return dict_manifestos


def manifestos_unicos_containers(dict_faltantes, dict_manifestos):
    manifestos = defaultdict(list)
    for numero, _id in dict_faltantes.items():
        manifesto = dict_manifestos.get(numero)
        if manifesto is not None:
            manifestos[manifesto].append(numero)
    return manifestos


def mongo_find_in(db, collection: str, fields: list, in_sets: list,
                  key_field: str):
    """Realiza um find $in in_set no db.collection e retorna dict.

    Args:
        db: conexão ao MongoDB com banco de dados selecionado "setted"

        collection: nome da coleção mongo para aplicar a "query"

        field: campo para aplicar a "filter by"

        in_set: lista ou conjunto de valores a passar para o operador "$in"

        key_field: campo para obter valores únicos, agrupa por este campo
        colocando-o como chave do dicionário de resposta.

    Returns:
        Dicionário de resultados formatado key:value(Somente campos não nulos)
        Conjuntos de set_field

    """
    result = OrderedDict()
    filtro = {}
    for field, in_set in zip(fields, in_sets):
        filtro[field] = {'$in': list(in_set)}
    print(filtro)
    cursor = db[collection].find(filtro)
    for linha in cursor:
        result[linha[key_field]] = {str(key): value for key, value in linha.items()
                                    if value is not None and key != '_id'}
    return result


def monta_mongo_dict(db, dict_manifestos_containeres):
    manifestos_set = set(dict_manifestos_containeres.keys())
    containers_set = set()
    for lista in dict_manifestos_containeres.values():
        for item in lista:
            containers_set.add(item)

    containers = mongo_find_in(db, 'CARGA.ContainerVazio', ['manifesto', 'container'],
                               [manifestos_set, containers_set], 'container')
    print(containers)
    manifestos = mongo_find_in(db, 'CARGA.Manifesto', ['manifesto'], [manifestos_set], 'manifesto')
    manifestos_escala = mongo_find_in(db, 'CARGA.EscalaManifesto', ['manifesto'], [manifestos_set], 'manifesto')
    escalas_set = set([value['escala'] for value in manifestos_escala.values()])
    escalas = mongo_find_in(db, 'CARGA.Escala', ['escala'], [escalas_set], 'escala')
    atracacoes = mongo_find_in(db, 'CARGA.AtracDesatracEscala', ['escala'], [escalas_set], 'escala')
    mongo_dict = {}
    for container, values in containers.items():
        ldict = {'vazio': True}
        ldict['container'] = values
        manifesto = values['manifesto']
        escala = manifestos_escala[manifesto]['escala']
        ldict['manifesto'] = manifestos[manifesto]
        ldict['escala'] = escalas[escala]
        ldict['atracacao'] = atracacoes[escala]
        mongo_dict[container] = ldict

    return mongo_dict


def manifesto_grava_fsfiles(db, data_inicio, data_fim):
    campo = 'manifesto'
    dict_faltantes = carga_faltantes(db, data_inicio, data_fim, campo)
    total_fsfiles = len(dict_faltantes.keys())
    logger.info('Total de contêineres sem %s de %s a %s: %s' %
                (campo, data_inicio, data_fim, total_fsfiles))

    dict_manifestos = manifestos_periodo(db, data_inicio, data_fim,
                                         get_cursor_vazios_mongo)
    dict_manifestos_containeres = manifestos_unicos_containers(
        dict_faltantes, dict_manifestos)
    total_manifestos = len(dict_manifestos_containeres.keys())
    logger.info('Total de manifestos para estes contêineres de %s a %s: %s' %
                (data_inicio, data_fim, total_manifestos))

    dados_carga = monta_mongo_dict(db,
                                   dict_manifestos_containeres,
                                   dict_faltantes)
    # dados_carga = {}
    # print(mongo_dict)
    for container, carga in dados_carga.items():
        _id = dict_faltantes[container]
        print(_id)
        db['fs.files'].update_one(
            {'_id': _id},
            {'$set': {'metadata.carga': carga}}
        )
    logger.info(
        'Resultado manifestos_grava_fsfiles '
        'Pesquisados %s. '
        'Encontrados %s .'
        % (total_fsfiles, len(dados_carga))
    )


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]

    # Testa mongo_dict
    row = db.fs.files.find_one({'metadata.contentType': 'image/jpeg',
                                'metadata.carga.vazio': True},
                               {'metadata.carga': 1})

    container = row['metadata']['carga']['container'][0]['container']
    dict_manifestos_containeres = {
        row['metadata']['carga']['manifesto'][0]['manifesto']: [container]
    }
    dict_faltantes = {
        container: row['_id']
    }
    dados_carga = monta_mongo_dict(db,
                                   dict_manifestos_containeres)

    dados_fsfiles = row['metadata']['carga']
    dados_carga = dados_carga[container]
    # print(dados_carga)
    # print(dados_fsfiles)
    for key, value in dados_carga.items():
        value_fsfiles = dados_fsfiles.get(key)
        if value_fsfiles is None:
            print('%s não existe' % key)
            continue
        if type(value) != type(value_fsfiles):
            print('Tipos diferentes: novo %s antigo %s. ' %
                  (type(value), type(value_fsfiles)))
        if value_fsfiles != value:
            print('Valores diferentes:')
            print(value)
            print(value_fsfiles)
    assert dados_carga == dados_fsfiles
