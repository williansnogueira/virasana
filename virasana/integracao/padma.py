"""Módulo com funcões de gravar um cache no fs.files.

Funções para consultar PADMA e gravar predições no metadata
do GridFS.

"""
import requests
from json.decoder import JSONDecodeError

from ajna_commons.flask.conf import PADMA_URL

BBOX_MODELS = ['ssd']


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
        return prediction['predictions'][0]['1'] < 0.5
    if model == 'peso':
        print(prediction)
        return prediction['predictions']['peso']
