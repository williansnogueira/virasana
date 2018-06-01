"""Exporta a base de imagens com metadata.

Exporta csv com campos chave da imagem.

Para gerar arquivos para treinamento de algoritmos de aprendizagem de máquina.

Usage:
    python exporta-img&data.py --start 2017-07-01 --end 2017-07-10  --out imgs

    start: data inicial

    end: data final

    out: diretório de destino. Se omitido, cria arquivo csv no diretório corrente.

    cache: Flag(True or false). Se fornecido, cria diretório imgs com gravação
    das imagens de contêiner recortadas.

"""
import csv
import os
from datetime import datetime, timedelta
from PIL import Image

import click
from pymongo import MongoClient

from ajna_commons.conf import ENCODE
from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from ajna_commons.utils.images import get_imagens_recortadas
from virasana.integracao import carga, CHAVES_GRIDFS
from virasana.exportacao.utils import campos_mongo_para_lista


BATCH_SIZE = 1000
today = datetime.today()
tendaysbefore = today - timedelta(days=10)
start = tendaysbefore.strftime('%Y-%m-%d')
end = today.strftime('%Y-%m-%d')
filename = 'teste.csv'


@click.command()
@click.option('--start', default=start, help='Data início do filtro. ' +
              'Padrão: ' + start)
@click.option('--end', default=end, help='Data final do filtro. ' +
              'Padrão: ' + end)
@click.option('--batch_size', default=BATCH_SIZE,
              help='Tamanho do lote. Padrão: ' + str(BATCH_SIZE))
@click.option('--cache', is_flag=True, help='Gerar diretório de imagens')
def export(start, end, batch_size, cache):
    """Exporta csv com campos chave da imagem.

    Para gerar arquivos para facilitar treinamento de algoritmos
    de aprendizagem de máquina.

    Filtra apenas imagens que já contenham predições da localização do
    contêiner e dentro das datas especificadas no formato (ano/mês/dia)

    """
    print('iniciando consulta')
    db = MongoClient(host=MONGODB_URI)[DATABASE]
    filtro = carga.ENCONTRADOS
    filtro['metadata.predictions.bbox'] = {'$exists': True, '$ne': None}
    filtro['metadata.dataescaneamento'] = {
        '$gt': datetime.strptime(start, '%Y-%m-%d'),
        '$lt': datetime.strptime(end, '%Y-%m-%d')
    }
    chaves = ['_id'] + CHAVES_GRIDFS + carga.CHAVES_CARGA
    lista = campos_mongo_para_lista(db, filtro, chaves, batch_size)
    with open(filename, 'w', encoding=ENCODE, newline='') as out:
        writer = csv.writer(out, quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writerows(lista)
    # Gera diretório com imagens
    if cache:
        try:
            os.mkdir('imgs')
        except FileExistsError:
            pass
        for linha in lista[1:]:
            print(linha[0])
            _id = linha[0]
            images = get_imagens_recortadas(db, _id)
            for index, im in enumerate(images):
                im.save(os.path.join('imgs', str(_id) + '_' + str(index)),
                        'JPEG')


if __name__ == '__main__':
    export()
