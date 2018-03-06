from datetime import datetime

from gridfs import GridFS
from pymongo import MongoClient

from virasana.workers.gridfs_functions import stats_resumo

# , xml_todict

db = MongoClient()['test']
fs = GridFS(db)

print(stats_resumo(db))
