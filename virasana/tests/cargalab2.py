from pymongo import MongoClient

from virasana.integracao import create_indexes, carga
from virasana.integracao.carga import busca_info_container, \
    dados_carga_grava_fsfiles

db = MongoClient()['test']


agg = db['CARGA.Escala'].aggregate(
    [{'$match': {'escala': {'$eq': '17000308905'}}},
     {'$lookup':
      {'from': 'CARGA.AtracDesatracEscala',
          'localField': 'escala',
          'foreignField': 'escala',
          'as': 'atracacoes'}
      },
     {'$lookup':
      {'from': 'CARGA.EscalaManifesto',
          'localField': 'escala',
          'foreignField': 'escala',
          'as': 'manifestos'}
      }
     ]
)

# agg = agg.find({'escala': '17000308905'})
for doc in agg:
    print(doc)
