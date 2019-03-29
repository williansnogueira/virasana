from collections import defaultdict, OrderedDict
from datetime import timedelta

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
    # print(filtro)
    cursor = db[collection].find(filtro)
    for linha in cursor:
        result[linha[key_field]] = {str(key): value for key, value in linha.items()
                                    if value is not None and key != '_id'}
    return result


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
