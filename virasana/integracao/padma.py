import io
import numpy as np
import requests
from json.decoder import JSONDecodeError

from bson.objectid import ObjectId
from gridfs import GridFS
from PIL import Image

from ajna_commons.flask.conf import PADMA_URL

BBOX_MODELS = ['ssd']


def recorta_imagem(image, coords=None):
    """Recebe uma imagem serializada em bytes, retorna Imagem cortada.
    Params:
        image: imagem em bytes (recebida via http ou via Banco de Dados)
        coords: (x0,y0,x1,y1)
    Returns:
        Um recorte no formato Image em bytes
    """
    if coords:
        PILimage = Image.open(io.BytesIO(image))
        im = np.asarray(PILimage)
        im = im[coords[0]:coords[2], coords[1]:coords[3]]
        PILimage = Image.fromarray(im)
        image_bytes = io.BytesIO()
        PILimage.save(image_bytes, 'JPEG')
        image_bytes.seek(0)
    return image_bytes


def mongo_image(db, image_id):
    """Lê imagem do Banco MongoDB. Retorna None se ID não encontrado."""
    fs = GridFS(db)
    _id = ObjectId(image_id)
    if fs.exists(_id):
        grid_out = fs.get(_id)
        image = grid_out.read()
        return image
    return None


def consulta_padma(image, model):
    """Monta e trata request para o PADMA.
        Args: """
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
    if model == 'vazio':
        return prediction['predictions'][0]['1'] < 0.5
    if model == 'peso':
        print(prediction)
        return prediction['predictions']['peso']
