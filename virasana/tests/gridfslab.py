from gridfs import GridFS
from pymongo import MongoClient

from virasana.integracao import stats_resumo

# , xml_todict

db = MongoClient()['test']
fs = GridFS(db)

print(stats_resumo(db))
