from collections import defaultdict, OrderedDict
from datetime import timedelta

from ajna_commons.flask.log import logger

from virasana.integracao.carga2 import carga_faltantes, mongo_find_in

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




def monta_mongo_dict(db, dict_manifestos_containeres, dict_faltantes):
    manifestos_set = set(dict_manifestos_containeres.keys())
    containers_set = set()
    for lista in dict_manifestos_containeres.values():
        for item in lista:
            containers_set.add(item)

    containers = mongo_find_in(db, 'CARGA.ContainerVazio', ['manifesto', 'container'],
                               [manifestos_set, containers_set], 'container')
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
