"""Script de linha de comando para integração do Sistema PADMA.

Script de linha de comando para fazer atualização 'manual'
dos metadados do módulo AJNA-PADMA nas imagens.

Importante: todos os modelos precisam atuar sobre um recorte da imagem
orginal, EXCETO os modelos treinados justamente para detectar este recorte
Assim, serão filtrados apenas os registros que possuam a chave bbox para
recorte, a menos que o modelo selecionado seja um dos modelos próprios para
detecção do objeto contêiner (lista BBOX_MODELS do integracao.padma).

Args:

    model: modelos a consultar

    batch_size: tamanho do lote de atualização/limite de registros da consulta

    sovazios: selecionar contêineres declarados como vazio somente

"""
import asyncio
import concurrent.futures
import time
import requests

import click


from virasana.views import db
from virasana.integracao.padma import (BBOX_MODELS, consulta_padma,
                                       interpreta_pred)

from ajna_commons.utils.images import mongo_image, recorta_imagem

BATCH_SIZE = 10000
MODEL = 'ssd'


def predict_cropped_images(predictions, image, model, _id):
    """Recorta imagens de acordo com bbox passada e consulta modelo.

    Para acessar algumas predições, é necessário recortar as imagens antes.
    Esta função combina as duas ações.

    """
    success = False
    for index, conteiner in enumerate(predictions):
        bbox = conteiner.get('bbox')
        if bbox:
            try:
                image_crop = recorta_imagem(image, bbox)
                pred = consulta_padma(image_crop, model)
                print(model, pred)
                if pred and pred['success']:
                    result = interpreta_pred(pred, model)
                    predictions[index][model] = result
                    success = True
            except TypeError as err:
                print('Erro ao recortar imagem', _id, str(err))
    return success, predictions


@click.command()
@click.option('--model', help='Modelo de predição a ser consultado',
              required=True)
@click.option('--batch_size',
              help='Tamanho do lote (padrão ' + str(BATCH_SIZE) + ')',
              default=BATCH_SIZE)
@click.option('--sovazios', is_flag=True,
              help='Processar somente vazios')
def update(model, batch_size, sovazios):
    """Script de linha de comando para integração de predições do PADMA."""
    filtro = {'metadata.contentType': 'image/jpeg'}
    if sovazios:
        filtro['metadata.carga.vazio'] = True
    # Modelo que cria uma caixa de coordenadas para recorte é pré requisito
    # para os outros modelos. Assim, outros modelos só podem rodar em registros
    # que já possuam o campo bbox (bbox: exists: True)
    if model in BBOX_MODELS:
        filtro['metadata.predictions.bbox'] = {'$exists': False}
    else:
        filtro['metadata.predictions.bbox'] = {'$exists': True}
        # filtro['metadata.predictions.' + model] = {'$eq': None}
        filtro['metadata.predictions.' + model] = {'$exists': False}

    aprocessar = 0  # db['fs.files'].find(filtro).count()
    print(aprocessar, ' arquivos sem predições com os parâmetros passados...')
    print(filtro)
    cursor = db['fs.files'].find(
        filtro, {'metadata.predictions': 1}).limit(batch_size)
    index = 0
    s0 = time.time()
    for registro in cursor:
        index += 1
        _id = registro['_id']
        image = mongo_image(db, _id)
        if image:
            print('Consultando modelo:', model,
                  'para o ID', _id, 'sequência', index)
            if model in BBOX_MODELS:
                pred_bbox = consulta_padma(image, model)
                print('Resultado da consulta:', pred_bbox)
                new_predictions = pred_bbox['predictions']
                success = pred_bbox and pred_bbox['success']
            else:
                predictions = registro['metadata'].get('predictions')
                success = False
                if predictions:
                    success, new_predictions = predict_cropped_images(
                        predictions,
                        image, model,
                        _id
                    )
                else:
                    print('Consulta retornou vazia! (modelo existe?)')
            if success:
                print('Gravando...', new_predictions, _id)
                db['fs.files'].update(
                    {'_id': _id},
                    {'$set': {'metadata.predictions': new_predictions}}
                )
    s1 = time.time()
    print('{0:.2f}'.format(s1 - s0))


def consulta_padma_retorna_id(_id, image, model):
    predictions = consulta_padma(image, model)
    return _id, predictions


def async_test():
    batch = 5

    async def main(images):
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch) as executor:
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor,
                    consulta_padma_retorna_id,
                    image[0], image[1], 'ssd'
                )
                for image in images
            ]
        index = 0
        for _id, response in await asyncio.gather(*futures):
            print(response)
            print('Consultou modelo:', 'ssd',
                  'image', _id,
                  'sequência', index)
            index += 1
            new_predictions = response['predictions']
            success = response and response['success']
            if success:
                print('Gravando...', new_predictions, _id)
                db['fs.files'].update(
                    {'_id': _id},
                    {'$set': {'metadata.predictions': new_predictions}}
                )

    filtro = {'metadata.contentType': 'image/jpeg'}
    filtro['metadata.predictions.bbox'] = {'$exists': False}
    batch_size = 50000
    print(batch_size, ' arquivos sem predições com os parâmetros passados...')
    cursor = db['fs.files'].find(
        filtro, {'metadata.predictions': 1}).limit(batch_size)
    print('Consulta ao banco efetuada, iniciando conexões ao Padma')
    index = 0
    images = []
    registros_vazios = 0
    s = time.time()
    for registro in cursor:
        pred_gravado = registro.get('metadata').get('predictions')
        if pred_gravado == []:
            registros_vazios += 1
            print('Pulando registros com anterior insucesso (vazios: []).',
                  'Registro ', registros_vazios)
            continue
        index += 1
        _id = registro['_id']
        image = mongo_image(db, _id)
        images.append((_id, image))
        if index % batch == 0:
            s0 = time.time()
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main(images))
            images = []
            s1 = time.time()
            print('Sequência real ..............  ', index,
                  '{0:.2f}'.format(s1 - s0), 'segundos')
    s1 = time.time()
    elapsed = s1 - s
    horas = elapsed // 3600
    minutos = (elapsed % 3600) / 60
    print('%d horas' % horas,
          '{0:02.2f}'.format(minutos), 'minutos',
          'registros vazios', registros_vazios,
          'registros processados', index)


if __name__ == '__main__':
    s0 = time.time()
    async_test()
    s1 = time.time()
    print('Tempo total de execução em segundos: {0:.2f}'.format(s1 - s0))
    # update()
