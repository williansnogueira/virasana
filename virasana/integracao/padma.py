"""Módulo com funcões de gravar um cache no fs.files.

Funções para consultar PADMA e gravar predições no metadata
do GridFS.

"""
import requests
from json.decoder import JSONDecodeError

import pymongo

from ajna_commons.flask.conf import PADMA_URL

BBOX_MODELS = ['ssd']

CHAVES_PADMA = [
    'metadata.predictions.vazio',
    'metadata.predictions.peso',
    'metadata.predictions.bbox'
]


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração."""
    for chave in CHAVES_PADMA:
        try:
            db['fs.files'].create_index(chave, sparse=True)
        except pymongo.errors.OperationFailure:
            pass


def consulta_padma(image, model):
    """Monta request para o PADMA. Trata JSON resposta.

    Args:
        image: bytes image
        model: nome do modelo a consultar

    Returns:
        dict com as predições

    """
    data = {}
    data['image'] = image
    headers = {}
    r = requests.post(PADMA_URL + '/predict?model=' + model,
                      files=data, headers=headers)
    try:
        result = r.json()
    except JSONDecodeError as err:
        print(err)
        return {'predictions': None, 'success': False}
    return result


def interpreta_pred(prediction, model):
    """Resume predições se necessário."""
    if model == 'vazio':
        return prediction['1'] < 0.5
    if model == 'peso':
        return prediction['peso']


if __name__ == '__main__':
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = pymongo.MongoClient(host=MONGODB_URI)[DATABASE]
    create_indexes(db)
