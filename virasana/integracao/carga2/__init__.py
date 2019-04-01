import datetime
from collections import OrderedDict
from enum import Enum


class Tipo(Enum):
    MANIFESTO = 1
    IMPORTACAO = 2
    EXPORTACAO = 3


FALTANTES = {'metadata.contentType': 'image/jpeg'}


def carga_faltantes(db, data_inicio, data_fim, tipo=Tipo.MANIFESTO):
    filtro = FALTANTES
    if tipo == Tipo.EXPORTACAO:
        filtro['metadata.carga.atracacao.conhecimento'] =  {'$exists': False}
    else:
        filtro['metadata.carga.atracacao.manifesto'] =  {'$exists': False}
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
