"""
For tests with PyMongo and MongoDB.

Tests for sintax and operations before putting into main code

"""
# import pprint
import timeit
import time
import sys
from datetime import datetime

# from gridfs import GridFS
from pymongo import MongoClient

from virasana.integracao import create_indexes, carga
from virasana.integracao.carga import busca_info_container, \
    dados_carga_grava_fsfiles


db = MongoClient()['test']

carga_dbs = ['CARGA.AtracDesatracEscala',
             'CARGA.CargaSolta',
             'CARGA.Conhecimento',
             'CARGA.Container',
             'CARGA.ContainerVazio',
             'CARGA.Escala',
             'CARGA.EscalaManifesto',
             'CARGA.Granel',
             'CARGA.Manifesto',
             'CARGA.ManifestoConhecimento',
             'CARGA.NCM',
             'CARGA.Parametros',
             'CARGA.Transbordo',
             'CARGA.Veiculo']

# EXCLUIR!!!!!!!!!!!!!!!!
# for dbname in carga_dbs:
#    db[dbname].remove({})
# Corrigir datas!!!!
"""for dbname in carga_dbs:
    linha = db[dbname].find_one({})
    for campo in linha:
        if campo.find('data') == 0:
            print(campo, linha[campo])
"""
# Com os dados acima poderiam ser corrigidos TODOS os campos de data.
# Por ora, corrigindo apenas dataatracacao que utilizamos para pesquisar

#################
# Criar índices
create_indexes(db)
carga.create_indexes(db)

if len(sys.argv) > 1 and sys.argv[1] == 'update':
    print('Começando a procurar por dados do CARGA a inserir')
    batch_size = 5000
    today = datetime.today()
    if len(sys.argv) > 2:
        year = int(sys.argv[2])
    else:
        year = today.year
    if len(sys.argv) > 3:
        month = int(sys.argv[3])
    else:
        month = today.month

    print(year, month)
    for day in range(1, 30, 5):
        data_inicio = datetime(year, month, day)
        print('Data início', data_inicio)
        tempo = time.time()
        dados_carga_grava_fsfiles(db, batch_size, data_inicio, days=4)
        tempo = time.time() - tempo
        print(batch_size, 'dados Carga do fs.files percorridos em ',
              tempo, 'segundos.',
              tempo / batch_size, 'por registro')

#############################################
# CARGA - testes para checar como pegar Info do Contêiner na Base CARGA
###
container = 'tclu2967718'
container_vazio = 'apru5774515'
data_escaneamento_false = datetime.utcnow()
data_escaneamento_true = datetime.strptime('17-08-02', '%y-%m-%d')
# Teste de desempenho
busca_info_container(db, container_vazio, data_escaneamento_false)
reps = 10
print('Início do teste de desempenho')
tempo = timeit.timeit(
    stmt='busca_info_container(db, container_vazio, data_escaneamento_false)',
    number=reps, globals=globals())
print('loops(TUDO - data falsa):', reps,
      'total time:', tempo, 'per loop:', tempo / reps)
tempo = timeit.timeit(
    stmt='busca_info_container(db, container, data_escaneamento_true)',
    number=reps, globals=globals())
print('loops(cheio):', reps, 'total time:', tempo, 'per loop:', tempo / reps)
tempo = timeit.timeit(
    stmt='busca_info_container(db, container_vazio, data_escaneamento_true)',
    number=reps, globals=globals())
print('loops(vazio):', reps, 'total time:', tempo, 'per loop:', tempo / reps)


# Ver dados retornados do CARGA
# print('Cheio')
# import pprint
# pprint.pprint(busca_info_container(db, container, data_escaneamento_true))
# print('Vazio')
# pprint.pprint(busca_info_container(db, container_vazio,
#  data_escaneamento_true))
# pprint.pprint(container)


# Teste com dados reais
data_inicio = datetime(2017, 6, 30)
file_cursor = db['fs.files'].find(
    {'metadata.dataescaneamento': {'$gt': data_inicio},
     'metadata.contentType': 'image/jpeg'})
count = file_cursor.count()
print(count, 'Total de imagens em fs.files', 'desde', data_inicio)

file_cursor = db['fs.files'].find(
    {'metadata.carga.vazio': None,
     'metadata.dataescaneamento': {'$gt': data_inicio},
     'metadata.contentType': 'image/jpeg'})
count = file_cursor.count()
print(count, 'Total de imagens sem metadata.carga', 'desde', data_inicio)

file_cursor = db['fs.files'].find(
    {'metadata.carga': 'NA',
     'metadata.contentType': 'image/jpeg'})
count = file_cursor.count()
print(count, 'Total de imagens com metadata.carga = "NA"',
      'desde', data_inicio)


linha = db['CARGA.AtracDesatracEscala'].find().sort(
    'dataatracacaoiso', 1).limit(1)
linha = next(linha)
print('Menor data de atracação (CARGA)', linha.get('dataatracacaoiso'))
linha = db['CARGA.AtracDesatracEscala'].find().sort(
    'dataatracacaoiso', -1).limit(1)
linha = next(linha)
print('Maior data de atracação (CARGA)', linha.get('dataatracacaoiso'))

linha = db['fs.files'].find(
    {'metadata.contentType': 'image/jpeg'}
).sort('metadata.dataescaneamento', 1).limit(1)
linha = next(linha)
print('Menor data de escaneamento (IMAGENS)',
      linha.get('metadata').get('dataescaneamento'))
linha = db['fs.files'].find(
    {'metadata.contentType': 'image/jpeg'}
).sort('metadata.dataescaneamento', -1).limit(1)
linha = next(linha)
print('Maior data de escaneamento (IMAGENS)',
      linha.get('metadata').get('dataescaneamento'))


# Procurar contêineres SEM imagem
"""qtde_conteineres = 10
lista_sem_imagens = []
container_cursor = db['CARGA.Container'].find(
    {}, ['container']).limit(qtde_conteineres)
for container in container_cursor:
    # print(container)
    # print(container['container'])
    file_cursor = db['fs.files'].find(
        {'metadata.carga.container.container': container['container']},
        ['_id'])
    # for file in file_cursor:
    #    print(file)
    if file_cursor.count() == 0:
        lista_sem_imagens.append(container['container'])
print(len(lista_sem_imagens), ' contêineres sem imagens de ',
      qtde_conteineres, ' procurados')
"""

container_cursor = db['CARGA.Container'].find(
    {}, ['container'])
print('Total de contêineres cheios importados do CARGA:',
      container_cursor.count())

container_vazio_cursor = db['CARGA.ContainerVazio'].find(
    {}, ['container'])
print('Total de contêineres vazios importados do CARGA:',
      container_vazio_cursor.count())

file_cursor = db['fs.files'].find(
    {'metadata.carga.container.container': {'$ne': None},
     'metadata.dataescaneamento': {'$gt': data_inicio},
     'metadata.contentType': 'image/jpeg'},
    ['metadata.carga.container.container'])
print('Total de imagens de container com metadata do CARGA:',
      file_cursor.count())

numero_container_set = set()
for container in container_cursor:
    numero_container_set.add(container['container'])
print('Total de contêineres únicos no CARGA:', len(numero_container_set))

numero_vazio_set = set()
for container in container_vazio_cursor:
    numero_vazio_set.add(container['container'])
print('Total de contêineres vazios únicos no CARGA:',
      len(numero_vazio_set))

print('Total de contêineres vazios e cheios únicos:',
      len(numero_container_set | numero_vazio_set))

# TODO: ver porque ficou uma lista no campo carga.container !!!???
imagem_container_set = set()
for container in file_cursor:
    lista_containers_file = container['metadata']['carga']['container']
    for numero in lista_containers_file:
        imagem_container_set.add(numero['container'])
print('Total de números de imagens de contêiner únicos:',
      len(imagem_container_set))

imagem_sem_container = (imagem_container_set -
                        numero_container_set) - numero_vazio_set
print('Números de contêiner nas imagens SEM contêiner correspondente' +
      ' na base CARGA, ignorando datas (tem que ser 0):',
      len(imagem_sem_container))
# for container in list(imagem_sem_container)[:10]:
#   print(container)


container_sem_imagem = (numero_container_set |
                        numero_vazio_set) - imagem_container_set
print('Números de contêineres no CARGA SEM numeração igual nas imagens:',
      len(container_sem_imagem))

