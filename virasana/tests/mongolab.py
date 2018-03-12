"""
For tests with PyMongo and MongoDB.

Tests for sintax and operations before putting into main code

"""
# import pprint
import timeit
import time
from datetime import datetime  # , timedelta

from gridfs import GridFS
from pymongo import MongoClient

from virasana.integracao.carga import busca_info_container, \
    create_indexes, dados_carga_grava_fsfiles

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
cursor = db['CARGA.AtracDesatracEscala'].find({'dataatracacaoiso': None})
for linha in cursor:
    dataatracacao = linha['dataatracacao']
    dataatracacaoiso = datetime.strptime(dataatracacao, '%d/%m/%Y')
    print(linha['_id'], dataatracacao, dataatracacaoiso)
    db['CARGA.AtracDesatracEscala'].update(
        {'_id': linha['_id']}, {'$set': {'dataatracacaoiso': dataatracacaoiso}}
    )


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


data_escaneamento = datetime(2017, 1, 1)

"""
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
data_inicio = datetime(2017, 6, 30)
file_cursor = db['fs.files'].find(
    {'metadata.dataescaneamento': {'$gt': data_inicio},
     'metadata.contentType': 'image/jpeg'})
count = file_cursor.count()
print(count, 'Total de imagens em fs.files', 'desde', data_inicio)

file_cursor = db['fs.files'].find(
    {'metadata.carga': None,
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

batch_size = 4000
for day in range(1, 30, 5):
    data_inicio = datetime(2017, 9, day)
    print('Data início', data_inicio)
    tempo = time.time()
    dados_carga_grava_fsfiles(db, batch_size, data_inicio, days=4)
    tempo = time.time() - tempo
    print(batch_size, 'dados Carga do fs.files percorridos em ', tempo, 'segundos.',
        tempo / batch_size, 'por registro')

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


####################
# Consultas para saneamento das tabelas...
print('############# Saneamento ##########')
# Registros duplicados no GridFS
# TODO: não aceitar duas vezes o mesmo arquivo ou fazer upsert
"""file_cursor = db['fs.files'].aggregate(
    [{'$group':
      {'_id': '$filename',
       'dups': {'$push': '$_id'},
       'count': {'$sum': 1}}},
     {'$match': {'count': {'$gt': 1}}}]
)
print(len(list(file_cursor)), ' Registros duplicados na tabela fs.files')
file_cursor = db['fs.files'].aggregate(
    [{'$group':
      {'_id': '$filename',
       'dups': {'$push': '$_id'},
       'count': {'$sum': 1}}},
     {'$match': {'count': {'$gt': 1}}}]
)
fs = GridFS(db)
for cursor in file_cursor:
    ids = cursor['dups']
    for _id in ids[1:]:
        fs.delete(_id)

print(len(list(file_cursor)), ' Registros duplicados na tabela fs.files')
"""
# Registros duplicados nas tabelas CARGA
# TODO: não aceitar duas vezes o mesmo registro ou fazer upsert
# TODO: Como registrar esta metadata???
# (Hard-coded não é ideal, em tese bhadrasana é dinâmico em relação às bases
# de origem)
"""bases = {'Conhecimento': '$conhecimento',
         'Manifesto': '$manifesto',
         'Container': '$container'}
for tabela, campo in bases.items():
    cursor = db['CARGA.' + tabela].aggregate(
        [{'$group':  {'_id': campo, 'count': {'$sum': 1}}},
         {'$match': {'count': {'$gt': 1}}}]
    )
    print(len(list(cursor)), ' Registros duplicados na tabela CARGA.' + tabela)
"""

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
print('Total de contêineres cheios:', container_cursor.count())

container_vazio_cursor = db['CARGA.ContainerVazio'].find(
    {}, ['container'])
print('Total de contêineres vazios:', container_vazio_cursor.count())

file_cursor = db['fs.files'].find(
    {'metadata.carga.container.container': {'$ne': None},
     'metadata.dataescaneamento': {'$gt': data_inicio},
     'metadata.contentType': 'image/jpeg'},
    ['metadata.carga.container.container'])
print('Total de imagens de container com metadata do carga:', file_cursor.count())

numero_container_set = set()
for container in container_cursor:
    numero_container_set.add(container['container'])
print('Total de contêineres únicos:', len(numero_container_set))

numero_vazio_set = set()
for container in container_vazio_cursor:
    numero_vazio_set.add(container['container'])
print('Total de contêineres vazios únicos:', len(numero_vazio_set))

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
print('Números de contêiner nas imagens SEM contêiner correspondente na base CARGA, ignorando datas (tem que ser 0):',
      len(imagem_sem_container))
# for container in list(imagem_sem_container)[:10]:
#   print(container)


container_sem_imagem = (numero_container_set |
                        numero_vazio_set) - imagem_container_set
print('Números de contêineres no CARGA SEM numeração igual nas imagens:', len(container_sem_imagem))

pipeline = [
    {'$lookup':
     {'from': 'CARGA.EscalaManifesto',
      'localField': 'Escala',
      'foreignField': 'Escala',
      'as': 'manifestos'
      }
     }
]
cursor = db['CARGA.AtracDesatracEscala'].aggregate(pipeline)
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
