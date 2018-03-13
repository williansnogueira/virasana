from datetime import datetime
from pymongo import MongoClient
# import pprint

# from virasana.integracao.carga import busca_info_container

# TODO: Query to know what CARGA Containers(Vazio e Cheio) do not have images!!

db = MongoClient()['test']

dataatracacaoiso = datetime(2017, 7, 21)


agg_vazios = db['CARGA.AtracDesatracEscala'].aggregate(
    [{'$match': {'dataatracacaoiso': {'$eq': dataatracacaoiso}}},
     {'$lookup':
      {'from': 'CARGA.EscalaManifesto',
          'localField': 'escala',
          'foreignField': 'escala',
          'as': 'manifestos'}
      },
     {'$lookup':
      {'from': 'CARGA.ContainerVazio',
       'localField': 'manifestos.manifesto',
       'foreignField': 'manifesto',
       'as': 'vazios'}
      },
     ])

agg_conhecimentos = db['CARGA.AtracDesatracEscala'].aggregate(
    [{'$match': {'dataatracacaoiso': {'$eq': dataatracacaoiso}}},
     {'$lookup':
      {'from': 'CARGA.EscalaManifesto',
          'localField': 'escala',
          'foreignField': 'escala',
          'as': 'manifestos'}
      },
     {'$lookup':
      {'from': 'CARGA.ManifestoConhecimento',
       'localField': 'manifestos.manifesto',
       'foreignField': 'manifesto',
       'as': 'conhecimentos'}
      },
     ]
)

conhecimentos = []
for escala in agg_conhecimentos:
    for conhecimento in escala['conhecimentos']:
        conhecimentos.append(conhecimento['conhecimento'])

agg_containers = db['CARGA.Container'].find(
    {'conhecimento': {'$in': conhecimentos}}
)


containers = []
for container in agg_containers:
    containers.append(container['container'])

vazios = []
for escala in agg_vazios:
    for vazio in escala['vazios']:
        vazios.append(vazio['container'])


print('conhecimentos', conhecimentos)
print('containers', containers)
print('vazios', vazios)
