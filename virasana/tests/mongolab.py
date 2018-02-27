"""
For tests with PyMongo and MongoDB
Test for sintax and operations before putting into main code

"""
from pymongo import MongoClient

db = MongoClient()['test']

cursor = db['CARGA.Container'].find()

print(list(cursor[:2]))

cursor = db['CARGA.Container'].find(
    {'container': {'$regex': '^BM'}}
)

cursor = db['CARGA.Container'].find()

print(list(cursor[:2]))


count = db['fs.files'].find().count()
print(count)

print(db.collection_names())