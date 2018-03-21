"""Gera uma base de imagens com pesos e volumes.

Exporta csv com id imagem, numero container, peso e volume
na quantidade q para o diretório out.

Usage:
    python pesovolexport.py -q 1000 -out ../pesos

    -q: quantidadade de registros a exportar.
    Se omitido, pega aleatoriamente 1.000 registros da base.

    -out: diretório de destino
    Se omitido, cria arquivo pesovolexport.csv no diretório corrente.

"""
import csv
import random
from datetime import datetime
from pymongo import MongoClient
from ajna_commons.flask.conf import (DATABASE, MONGODB_URI)
from virasana.integracao import carga, peso_container_documento, volume_container

db = MongoClient(host=MONGODB_URI)[DATABASE]
print('iniciando consulta')


filtro = carga.ENCONTRADOS
filtro['metadata.dataescaneamento'] = {'$gt': datetime(
    2017, 8, 5), '$lt': datetime(2017, 8, 20)}
cursor = db['fs.files'].find(
    filtro,
    {'metadata.carga.container.container': 1,
     'metadata.carga.container.pesobrutoitem': 1,
     'metadata.carga.container.volumeitem': 1})

containers = [['_id', 'numero', 'peso', 'volume']]
for linha in cursor:
    item = linha['metadata']['carga']['container']
    if item[0].get('pesobrutoitem'):
        peso = float(item[0]['pesobrutoitem'].replace(',', '.'))
        volume = float(item[0]['volumeitem'].replace(',', '.'))
        containers.append(
            [linha['_id'],
             linha['metadata']['carga']['container'][0]['container'],
             peso, volume])
    #####
    # Rever importaçao! Pelo jeito está puxando contêiner 2 vezes,
    # 1 para MBL e outra para HBL
    # peso = 0.
    # volume = 0.
    # for item in linha['metadata']['carga']['container']:
    #    if item.get('pesobrutoitem'):
    #        peso += float(item['pesobrutoitem'].replace(',', '.'))
    #        volume += float(item['volumeitem'].replace(',', '.'))
    # if peso != 0.:

print(len(containers))

containers = random.sample(containers, 1000)
with open('pesovolexport.csv', 'w') as out:
    writer = csv.writer(out)
    writer.writerows(containers)
