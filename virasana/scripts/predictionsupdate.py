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
import click

from virasana.views import db
from virasana.integracao.padma import (BBOX_MODELS, consulta_padma,
                                       interpreta_pred, mongo_image,
                                       recorta_imagem)

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
    for registro in cursor:
        index += 1
        _id = registro['_id']
        image = mongo_image(db, _id)
        if image:
            print('Consultando modelo:', model, 'para o ID', _id, 'sequência', index)
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


if __name__ == '__main__':
    update()
