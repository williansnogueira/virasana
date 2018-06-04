"""Script de linha de comando para integração do Sistema PADMA.

Script de linha de comando para fazer atualização 'manual'
dos metadados do módulo AJNA-PADMA nas imagens.

Importante: todos os modelos precisam atuar sobre um recorte da imagem
orginal, EXCETO os modelos treinados justamente para detectar este recorte
Assim, serão filtrados apenas os registros que possuam a chave bbox para
recorte, a menos que o modelo selecionado seja um dos modelos próprios para
detecção do objeto contêiner (lista BBOX_MODELS do integracao.padma).

Args:

    modelo: modelos a consultar

    t: tamanho do lote de atualização/limite de registros da consulta

    q: quantidade de consultas simultâneas (mais rápido até limite do Servidor)

    sovazios: selecionar contêineres declarados como vazio somente

    force: Tentar mesmo se consulta anterior a este registro falhou.
    Obs: em caso de consulta retornar vazia (não detectou resultado),
    o script irá gravar um valor em branco [] no campo destino e irá
    pular este registro nas próximas atualizações. Habilitar force
    processa estes registros novamente.

"""
import asyncio
import concurrent.futures
import time
import requests
from collections import namedtuple

import click
from bson import ObjectId

from virasana.views import db
from virasana.integracao.padma import (BBOX_MODELS, consulta_padma,
                                       interpreta_pred)

from ajna_commons.utils.images import mongo_image, recorta_imagem


def monta_filtro(model: str, sovazios: bool)-> dict:
    """Retorna filtro para MongoDB."""
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
        filtro['metadata.predictions.' + model] = {'$exists': False}
    return filtro


def cropped_images(predictions: dict, image: bytes, _id: int)-> list:
    """Recorta imagens de acordo com bbox passada.

    Para acessar algumas predições, é necessário recortar as imagens antes.

    Args: 
        predictions: dicionário com as predições
        image: imagem do contêiner original, em bytes
        _id: id do MongoDB se existir. Apenas para reportar erro

    Returns:
        lista de imagem(ns) recortada[s]
    """
    images = []
    for prediction in predictions:
        bbox = prediction.get('bbox')
        if bbox:
            try:
                image_crop = recorta_imagem(image, bbox)
                images.append(image_crop)
            except TypeError as err:
                print('Erro ao recortar imagem', _id, str(err))
    return images


ImageID = namedtuple('ImageID', ['id', 'content', 'ind'])


def append_images(model: str, _id: ObjectId, image: bytes,
                  predictions: dict, images: list)-> list:
    """Anexa(append) image e _id em lista(images) fornecida e retorna lista.

    Se model for BBOX, simplesmente anexa imagem original. 
    Se model for outro tipo, recorta BBOX antes utilizando predictions BBOX
    já vinculadas à imagem _id e anexa uma ou duas imagens resultantes.
    """
    if model in BBOX_MODELS:
        images.append(ImageID(_id, image, 0))
    else:
        if predictions:
            for ind, cropped_image in \
                    enumerate(cropped_images(predictions, image, _id)):
                images.append(ImageID(_id, cropped_image, ind))

    return images


def mostra_tempo_final(s_inicial, registros_vazios, registros_processados):
    """Exibe mensagem de tempo decorrido."""
    s1 = time.time()
    elapsed = s1 - s_inicial
    horas = elapsed // 3600
    minutos = (elapsed % 3600) // 60
    segundos = elapsed % 60
    print('%d:%d:%d' % (horas, minutos, segundos),
          'registros vazios', registros_vazios,
          'registros processados', registros_processados)


def consulta_padma_retorna_id(_id, image, ind, model):
    predictions = consulta_padma(image, model)
    return _id, predictions, ind


async def fazconsulta(images, model, pred_gravado):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor,
                consulta_padma_retorna_id,
                image['id'], image['content'], image['ind'],
                model
            )
            for image in images
        ]
    seq = 0
    for _id, response, ind in await asyncio.gather(*futures):
        print('Consultou modelo:', 'ssd', 'image', _id,
              'sequência', seq, 'resultado:', response)
        if response and response['success']:
            # Por ora, uma consulta BBOX APAGA predições anteriores
            if model in BBOX_MODELS:
                new_predictions = response['predictions']
            else:
                new_predictions = pred_gravado
                result = interpreta_pred(response['predictions'], model)
                new_predictions[ind][model] = result
            seq += 1
            print('Gravando...', new_predictions, _id, seq)
            db['fs.files'].update(
                {'_id': _id},
                {'$set': {'metadata.predictions': new_predictions}}
            )
        else:
            print('Consulta retornou vazia! (modelo existe?)')


BATCH_SIZE = 10000
MODEL = 'ssd'
THREADS = 4


@click.command()
@click.option('--modelo', help='Modelo de predição a ser consultado',
              required=True)
@click.option('--t',
              help='Tamanho do lote (padrão ' + str(BATCH_SIZE) + ')',
              default=BATCH_SIZE)
@click.option('--q',
              help='Quantidade de consultas paralelas (padrão ' +
              str(THREADS) + ')',
              default=THREADS)
@click.option('--sovazios', is_flag=True,
              help='Processar somente vazios')
@click.option('--force', is_flag=True,
              help='Tentar mesmo se consulta anterior a este registro falhou.')
def async_update(modelo, t, q, sovazios, force):
    """Consulta padma e grava predições de retorno no MongoDB."""
    filtro = monta_filtro(modelo, sovazios)
    batch_size = t
    threads = q
    print(batch_size, ' arquivos sem predições com os parâmetros passados...')
    cursor = db['fs.files'].find(
        filtro, {'metadata.predictions': 1}).limit(batch_size)
    print('Consulta ao banco efetuada, iniciando conexões ao Padma')
    registros_processados = 0
    registros_vazios = 0
    s_inicio = time.time()
    images = []
    for registro in cursor:
        _id = registro['_id']
        pred_gravado = registro.get('metadata').get('predictions')
        if not force:
            if pred_gravado == []:
                registros_vazios += 1
                print('Pulando registros com anterior insucesso (vazios: []).',
                      'Registro ', registros_vazios)
                continue
        registros_processados += 1
        image = mongo_image(db, _id)
        images = append_images(
            model=modelo, _id=_id, image=image, predictions=pred_gravado, images=images)
        print(images)
        if registros_processados % threads == 0:
            s0 = time.time()
            loop = asyncio.get_event_loop()
            loop.run_until_complete(fazconsulta(images, modelo, pred_gravado))
            images = []
            s1 = time.time()
            print('Sequência real ..............  ', registros_processados,
                  '{0:.2f}'.format(s1 - s0), 'segundos')
    mostra_tempo_final(s_inicio, registros_vazios, registros_processados)


if __name__ == '__main__':
    s0 = time.time()
    async_update()
    s1 = time.time()
    print('Tempo total de execução em segundos: {0:.2f}'.format(s1 - s0))
    # update()
