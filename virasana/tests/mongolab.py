"""
For tests with PyMongo and MongoDB.

Tests for sintax and operations before putting into main code

"""
import pprint
import timeit
import time
from datetime import datetime, timedelta

from pymongo import MongoClient

from virasana.workers.carga_functions import (busca_info_container,
                                              create_indexes,
                                              dados_carga_grava_fsfiles)

db = MongoClient()['test']
#################
# Criar índices
create_indexes(db)


#############################################
# CARGA - testes para checar como pegar Info do Contêiner na Base CARGA
###
container = 'tclu2967718'
container_vazio = 'apru5774515'
data_escaneamento_false = datetime.utcnow()
data_escaneamento_true = datetime.strptime('17-08-02', '%y-%m-%d')
# Teste de desempenho
"""
reps = 3
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

# teste de função
assert busca_info_container(db, container, data_escaneamento_false) == {}
assert busca_info_container(db, container, data_escaneamento_true) != {}
assert busca_info_container(db, container_vazio, data_escaneamento_false) == {}
assert busca_info_container(db, container_vazio, data_escaneamento_true) != {}


"""
data_escaneamento = datetime(2017, 1, 1)

""""
Exemplo de como criar dados para teste:


data_escalas = data_escaneamento - timedelta(days=1)
data_escala_4 = data_escaneamento - timedelta(days=4)
db['fs.files'].insert({'metadata.numeroinformado': 'cheio',
                       'metadata.dataescaneamento': data_escaneamento})
db['fs.files'].insert({'metadata.numeroinformado': 'cheio', 'conhecimento': 1})


db['CARGA.Container'].insert({'container': 'cheio', 'conhecimento': 1})
db['CARGA.Container'].insert(
    {'container': 'semconhecimento', 'conhecimento': 9})
db['CARGA.Container'].insert({'container': 'semescala', 'conhecimento': 3})
db['CARGA.Container'].insert(
    {'container': 'escalaforadoprazo', 'conhecimento': 4})
db['CARGA.ContainerVazio'].insert({'container': 'vazio'})
db['CARGA.Conhecimento'].insert({'conhecimento': 1})
db['CARGA.Conhecimento'].insert({'conhecimento': 2})
db['CARGA.Conhecimento'].insert({'conhecimento': 3})
db['CARGA.Conhecimento'].insert({'conhecimento': 3})
db['CARGA.ConhecimentoManifesto'].insert({'conhecimento': 1, 'manifesto': 1})
db['CARGA.ConhecimentoManifesto'].insert({'conhecimento': 2, 'manifesto': 2})
db['CARGA.ConhecimentoManifesto'].insert({'conhecimento': 3, 'manifesto': 3})
db['CARGA.ConhecimentoManifesto'].insert({'conhecimento': 4, 'manifesto': 4})
db['CARGA.ConhecimentoManifesto'].insert({'conhecimento': 3, 'manifesto': 32})
db['CARGA.ManifestoEscala'].insert({'manifesto': 1, 'escala': 1})
db['CARGA.ManifestoEscala'].insert({'manifesto': 2, 'escala': 2})
db['CARGA.ManifestoEscala'].insert({'manifesto': 3, 'escala': 3})
db['CARGA.ManifestoEscala'].insert({'manifesto': 4, 'escala': 4})
db['CARGA.AtracDesatracEscala'].insert(
    {'escala': 4, 'dataatracacao': data_escala_4})
"""

# Ver dados retornados do CARGA
# print('Cheio')
# pprint.pprint(busca_info_container(db, container, data_escaneamento_true))
# print('Vazio')
# pprint.pprint(busca_info_container(db, container_vazio,
#  data_escaneamento_true))
# pprint.pprint(container)


# Teste com dados reais
data_inicio = datetime(2017, 7, 1)
file_cursor = db['fs.files'].find(
    {'metadata.carga': None,
     'metadata.dataescaneamento': {'$gt': data_inicio}})
count = file_cursor.count()
print(count, 'Total de arquivos sem metadata.carga', 'desde', data_inicio)
file_cursor = db['fs.files'].find(
    {'metadata.carga': 'NA'})
count = file_cursor.count()
print(count, 'Total de arquivos com metadata.carga = "NA"', 'desde', data_inicio)
batch_size = 60000
# dados_carga_grava_fsfiles(db, 100, data_inicio)
tempo = time.time()
dados_carga_grava_fsfiles(db, batch_size, data_inicio, force_update=True)
tempo = time.time() - tempo
print('Dados Carga do fs.files percorridos em ', tempo, 'segundos.',
      tempo / batch_size, 'por registro')

linha = db['CARGA.AtracDesatracEscala'].find().sort('dataatracacao').limit(1)
linha = next(linha)
print('Menor data de atracação (CARGA)', linha.get('dataatracacao'))
linha = db['CARGA.AtracDesatracEscala'].find().sort(
    'dataatracacao', -1).limit(1)
linha = next(linha)
print('Maior data de atracação (CARGA)', linha.get('dataatracacao'))

linha = db['fs.files'].find().sort('metadata.dataescaneamento', 1).limit(1)
linha = next(linha)
print('Menor data de importação (IMAGENS)',
      linha.get('metadata').get('dataescaneamento'))
linha = db['fs.files'].find().sort('metadata.dataescaneamento', -1).limit(1)
linha = next(linha)
print('Maior data de importação (IMAGENS)',
      linha.get('metadata').get('dataescaneamento'))


# Exemplo de script para atualizar um campo com base em outro
#  caso dados mudem de configuração, campos mudem de nome, etc
"""cursor = db['fs.files'].find({'metadata.dataescaneamento': None})
print(cursor.count())
for linha in cursor:
    data = linha.get('metadata').get('dataimportacao')
    db['fs.files'].update(
        {'_id': linha['_id']},
        {'$set': {'metadata.dataescaneamento': data}}
    )
"""
