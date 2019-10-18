import datetime
import io
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np
import streamlit as st


from gridfs import GridFS
from bson import ObjectId
from PIL import Image
from scipy.stats import zscore
from sklearn.metrics.pairwise import cosine_distances, euclidean_distances

from ajna_commons.utils.images import mongo_image
from virasana.db import mongodb

st.title('Busca anomalias em um lote (Lote=CE-Mercante=BL)')


# @st.cache(ignore_hash=True)
def get_conhecimentos_um_ncm(inicio: datetime, fim: datetime) -> defaultdict(set):
    result = set()
    query = {'metadata.contentType': 'image/jpeg',
             'metadata.carga.ncm': {'$size': 1},
             'metadata.carga.container.indicadorusoparcial': {'$ne': 's'},
             'metadata.dataescaneamento': {'$gte': inicio, '$lt': fim}
             }
    projection = {'metadata.carga.conhecimento': 1}
    cursor = mongodb['fs.files'].find(query, projection)
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


def get_indexes_and_ids(db, conhecimentos: list):
    """Retorna todas as imagens vinculadas à lista de conhecimentos passada"""
    conhecimentos_ids = defaultdict(set)
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
                conhecimentos_ids[conhecimento].add(linha['_id'])
                ids_indexes[linha['_id']] = {'index': index, 'ncm': ncms[0]['ncm']}
    return conhecimentos_ids, ids_indexes


def norm_distance(array, search):
    return np.linalg.norm(array - search)


def euclidean_sum(array, search):
    return euclidean_distances(array, [search]).sum(axis=0)


def cosine_sum(array, search):
    return cosine_distances(array, [search]).sum(axis=0)


def outlier_index(indexes, distance_function=cosine_sum, zscores=3):
    size = indexes.shape[0]
    distances = np.zeros((size, 1), dtype=np.float32)
    for ind in range(size):
        linha = indexes[ind, :]
        try:
            distances[ind] = distance_function(indexes, linha)
            distances[ind, ind] = 0.01
        except Exception as err:
            print(err)
            # print(indexes)
            return np.array([])
    # print(distances),m.
    return np.where(zscore(distances) > zscores)[0]


def filtra_anomalias(conhecimentos_ids, ids_indexes):
    conhecimentos_anomalia = []
    for conhecimento, ids in conhecimentos_ids.items():
        indexes = [ids_indexes[id]['index'] for id in ids]
        array_indexes = np.array(indexes)
        try:
            outliers = outlier_index(array_indexes)
        except Exception as err:
            print(err)
        if outliers.shape[0] > 0:
            conhecimentos_anomalia.append(conhecimento)
    return conhecimentos_anomalia

def plot_imagens(imagens):
    plt.figure(figsize=(14, 16))
    for ind, imagem in enumerate(imagens, 1):
        img = Image.open(io.BytesIO(imagem))
        plt.subplot(len(imagens), 1, ind)
        plt.imshow(img)


st.write('Inicio')
inicio = datetime.datetime(2017, 7, 1)
fim = datetime.datetime(2017, 7, 3)
conhecimentos = get_conhecimentos_um_ncm(inicio, fim)
st.write('Total de conhecimentos do período: %s' % len(conhecimentos))
conhecimentos_ids, ids_indexes = get_indexes_and_ids(mongodb, conhecimentos)
conhecimentos_anomalia = filtra_anomalias(conhecimentos_ids, ids_indexes)
st.write('Total de conhecimentos filtrados: %s' % len(conhecimentos_anomalia))
option = st.selectbox('Qual conhecimento?', conhecimentos_anomalia)

imagens = []
for id in conhecimentos_ids[option]:
    img = mongo_image(mongodb, ObjectId(id))
    if img:
        imagens.append(img)

# print(imagens)
plot_imagens(imagens)

# db.fs.files.find({'metadata.contentType': 'image/jpeg', 'metadata.carga.ncm': {'$size': 1}, 'metadata.carga.container.indicadorusoparcial': {'$ne': 's'}, 'metadata.dataescaneamento': {'$gte': ISODate("2017-07-01"), '$lte': ISODate("2017-07-08")}})
