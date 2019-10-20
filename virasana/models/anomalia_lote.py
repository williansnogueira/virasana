import datetime
from collections import defaultdict

import numpy as np

from scipy.stats import zscore
from sklearn.metrics.pairwise import cosine_distances, euclidean_distances


def norm_distance(array, search):
    return np.linalg.norm(array - search)


def euclidean_sum(array, search):
    return euclidean_distances(array, [search]).sum(axis=0)


def cosine_sum(array, search):
    return cosine_distances(array, [search]).sum(axis=0)


def get_distances(indexes, distance_function=cosine_sum):
    size = indexes.shape[0]
    distances = np.zeros((size, 1), dtype=np.float32)
    for ind in range(size):
        linha = indexes[ind, :]
        distances[ind] = distance_function(indexes, linha)
    return distances


def get_zscores(indexes, distance_function=cosine_sum):
    return zscore(get_distances(indexes, distance_function))


def outlier_index(indexes, max_zscores=3, distance_function=cosine_sum):
    zscores = get_zscores(indexes, distance_function)
    return np.where(zscores > max_zscores)[0]


def get_conhecimentos_um_ncm(db, inicio: datetime, fim: datetime) -> set:
    """Consulta apenas contêineres(imagens) com um NCM e retorna seus conhecimentos."""
    result = set()
    query = {'metadata.contentType': 'image/jpeg',
             'metadata.carga.ncm': {'$size': 1},
             'metadata.carga.container.indicadorusoparcial': {'$ne': 's'},
             'metadata.dataescaneamento': {'$gte': inicio, '$lt': fim}
             }
    projection = {'metadata.carga.conhecimento': 1}
    cursor = db['fs.files'].find(query, projection)
    for linha in cursor:
        conhecimentos = linha.get('metadata').get('carga').get('conhecimento')
        if conhecimentos:
            if isinstance(conhecimentos, str):
                conhecimentos = [conhecimentos]
            for conhecimento in conhecimentos:
                tipo = conhecimento.get('tipo')
                if tipo != 'mbl':
                    conhecimento = conhecimento.get('conhecimento')
                    result.add(conhecimento)
    return result


def get_conhecimentos_zscore(db, inicio: datetime, fim: datetime, min_zscore=3) -> set:
    """Consulta apenas contêineres(imagens) com um NCM e retorna seus conhecimentos."""
    result = set()
    query = {'metadata.contentType': 'image/jpeg',
             'metadata.zscore': {'$gte': min_zscore},
             'metadata.dataescaneamento': {'$gte': inicio, '$lt': fim}
             }
    projection = {'metadata.carga.conhecimento': 1}
    cursor = db['fs.files'].find(query, projection)
    for linha in cursor:
        conhecimentos = linha.get('metadata').get('carga').get('conhecimento')
        if conhecimentos:
            if isinstance(conhecimentos, str):
                conhecimentos = [conhecimentos]
            for conhecimento in conhecimentos:
                tipo = conhecimento.get('tipo')
                if tipo != 'mbl':
                    conhecimento = conhecimento.get('conhecimento')
                    result.add(conhecimento)
    return result


def get_indexes_and_ids_conhecimentos(db, conhecimentos: list):
    """Retorna todas as imagens vinculadas à lista de conhecimentos passada

    Aqui são necessários dois passos adicionais:

    1. Buscar todas as imagens de cada conhecimento, já que o filtro por período de escaneamento
    pode ter excluído alguma

    2. Filtrar novamente o NCM único, para garantir que é único para o lote todo, não somente para
    um contêiner

    """
    conhecimentos_ids = defaultdict(list)
    ids_indexes = dict()
    projection = {'_id': 1, 'metadata.predictions': 1, 'metadata.carga.ncm.ncm': 1}
    for conhecimento in conhecimentos:
        query = {'metadata.carga.conhecimento.conhecimento': conhecimento}
        cursor = db['fs.files'].find(query, projection)
        for linha in cursor:
            predictions = linha['metadata'].get('predictions')
            if predictions:
                index = predictions[0].get('index')
            ncms = linha['metadata'].get('carga').get('ncm')
            if index and ncms and len(ncms) == 1:
                conhecimentos_ids[conhecimento].append(linha['_id'])
                ids_indexes[linha['_id']] = {'index': index, 'ncm': ncms[0]['ncm']}
    return conhecimentos_ids, ids_indexes


def get_ids_score_conhecimento_zscore(db, conhecimentos: list):
    """Retorna todas as imagens vinculadas à lista de conhecimentos passada

    Aqui é necessário um passo adicional: buscar todas as imagens de cada conhecimento,
    já que o filtro por período de escaneamento pode ter excluído alguma

    """
    conhecimentos_idszscore = defaultdict(list)
    ids_zscores = dict()
    projection = {'_id': 1, 'metadata.zscore': 1}
    for conhecimento in conhecimentos:
        query = {'metadata.carga.conhecimento.conhecimento': conhecimento}
        cursor = db['fs.files'].find(query, projection)
        for linha in cursor:
            zscore_dict = {'_id': linha['_id'],
                           'zscore': linha['metadata'].get('zscore')}
            conhecimentos_idszscore[conhecimento].append(zscore_dict)
    return conhecimentos_idszscore


def grava_zcores(db, conhecimentos_ids, ids_indexes):
    for conhecimento, ids in conhecimentos_ids.items():
        indexes = [ids_indexes[_id]['index'] for _id in ids]
        array_indexes = np.array(indexes)
        zscores = get_zscores(array_indexes)
        for ind, _id in enumerate(ids):
            db['fs.files'].update_one(
                {'_id': _id},
                {'$set': {'metadata.zscore': float(zscores[ind])}})


def filtra_anomalias(conhecimentos_ids, ids_indexes):
    conhecimentos_anomalia = {}
    for conhecimento, ids in conhecimentos_ids.items():
        indexes = [ids_indexes[id]['index'] for id in ids]
        array_indexes = np.array(indexes)
        outliers = outlier_index(array_indexes)
        if outliers.shape[0] > 0:
            conhecimentos_anomalia[conhecimento] = outliers
    return conhecimentos_anomalia
