from collections import defaultdict, OrderedDict
from datetime import date, datetime, time, timedelta
from virasana.integracao.carga2 import DELTA_VAZIO


def get_cursor_vazios_mongo(db, start, end):
    AGREGATE_VAZIO = [
    {'$match': {'dataatracacaoiso':
                {'$gte': start  - timedelta(days=DELTA_VAZIO),
                 '$lte': end + timedelta(days=DELTA_VAZIO)}}},
    {'$lookup': {
        'from': 'CARGA.EscalaManifesto',
        'localField': 'escala',
        'foreignField': 'escala',
        'as': 'lista_manifestos'
    }},
    {'$project': {'escala':1, 'dataatracacaoiso': 1,
                  'lista_manifestos.manifesto': 1}},
    {'$lookup': {
        'from': 'CARGA.ContainerVazio',
        'localField': 'lista_manifestos.manifesto',
        'foreignField': 'manifesto',
        'as': 'containers_vazios'
    }},
    {'$project': {'escala':1, 'dataatracacaoiso': 1,
                  'containers_vazios.container': 1,
                  'containers_vazios.manifesto': 1}},
    {'$sort': {'containers_vazios.container': 1}}
    ]

    return db['CARGA.AtracDesatracEscala'].aggregate(AGREGATE_VAZIO)

def manifestos_periodo(start, end, cursor_function):
    dict_manifestos = dict()
    fs_cursor = cursor_function(start, end)
    for lista in fs_cursor:
        for linha in lista['containers_vazios']:
            manifesto = linha['manifesto']
            dict_manifestos[linha['container']] = manifesto
    return dict_manifestos

dict_manifestos = manifestos_periodo(start, end, get_cursor_vazios_mongo)


def manifestos_unicos_containers(dict_faltantes, dict_manifestos):
    manifestos = defaultdict(list)
    for numero, _id in dict_faltantes.items():
        manifesto = dict_manifestos.get(numero)
        if manifesto is not None:
            manifestos[manifesto].append(numero)
    return manifestos


dict_manifestos_containeres = manifestos_unicos_containers(dict_faltantes, dict_manifestos)
