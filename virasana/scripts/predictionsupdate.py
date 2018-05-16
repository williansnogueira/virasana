"""Script de linha de comando para integração do Sistema PADMA.

Script de linha de comando para fazer atualização 'manual'
dos metadados do módulo AJNA-PADMA nas imagens.

Importante: todos os modelos precisam atuar sobre um recorte da imagem
orginal, EXCETO os modelos treinados justamente para detectar este recorte
Assim, serão filtrados apenas os registros que possuam a chave bbox para
recorte, a menos que o modelo selecionado seja um dos modelos próprios para
detecção do objeto contêiner.

Args:
    model: modelos a consultar
    batch_size: tamanho do lote de atualização/limite de registros da consulta
    sovazios: selecionar contêineres declarados como vazio somente
"""
import click
import io
import os
import numpy as np
import requests

from bson.objectid import ObjectId
from gridfs import GridFS
from PIL import Image

from virasana.views import db
from ajna_commons.flask.conf import PADMA_URL


def recorta_imagem(image, coords=None):
    """Recebe uma imagem serializada em bytes, retorna PIL Image.
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


def mongo_image(image_id):
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
    result = r.json()
    return result


def interpreta_pred(prediction, model):
    if model == 'vazio':
        return prediction['predictions'][0]['1'] < 0.5
    if model == 'peso':
        print(prediction)
        return None


BBOX_MODELS = ['ssd']
BATCH_SIZE = 10000
MODEL = 'ssd'


@click.command()
@click.option('--model', default=MODEL, help='Modelo de predição a ser consultado')
@click.option('--batch_size', default=BATCH_SIZE,
              help='Tamanho do lote - padrão ' + str(BATCH_SIZE))
@click.option('--sovazios', default=False,
              help='Processar somente vazios')
def update(model, batch_size, sovazios):
    """Script de linha de comando para integração de predições do módulo PADMA."""
    filtro = {}
    if sovazios:
        filtro['metadata.carga.vazio'] = True
    # Modelo que cria uma caixa de coordenadas para recorte é pré requisito
    # para os outros modelos. Assim, outros modelos só podem rodar em registros
    # que já possuam o campo bbox
    if model in BBOX_MODELS:
        filtro['metadata.predictions.bbox'] = {'$exists': False}
    else:
        filtro['metadata.predictions.bbox'] = {'$exists': True}
        filtro['metadata.predictions.'+model] = {'$exists': False}

    aprocessar = db['fs.files'].find(filtro).count()
    print(aprocessar, ' arquivos sem predições com os parâmetros passados...')

    cursor = db['fs.files'].find(filtro, {'metadata.predictions': 1}).limit(batch_size)
    for registro in cursor:
        _id = registro['_id']
        image = mongo_image(_id)
        if image:
            print('Consultando modelo:', model, 'para o ID', _id)
            if model in BBOX_MODELS:
                pred_bbox = consulta_padma(image, model)
                print('Resultado da consulta:', pred_bbox)
                predictions = pred_bbox['predictions']
                if pred_bbox and pred_bbox['success'] == True and predictions:
                    print('Gravando...', predictions)
                    db['fs.files'].update(
                        {'_id': _id},
                        {'$set': {
                            'metadata.predictions': predictions}}
                    )
            else:
                predictions = registro['metadata.predictions']
                for index, conteiner in enumerate(predictions):
                    bbox = conteiner.get('bbox')
                    if bbox:
                        image = recorta_imagem(image, bbox)
                        # image.save(os.path.join('.', str(_id) + '.jpg'), 'JPEG', quality=100)
                        pred = consulta_padma(image, model)
                        print(model, pred)
                        if pred and pred['success'] == True:
                            result = interpreta_pred(pred, model)
                            predictions[index][model] = result
                db['fs.files'].update(
                    {'_id': _id},
                    {'$set': {'metadata.predictions': predictions}}
                )


if __name__ == '__main__':
    update()
