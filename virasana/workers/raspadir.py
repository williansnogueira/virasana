# Código do celery task está no arquivo principal virasana.virasana.py
# TODO: resolver circular import para ativar este arquivo e deixar
# código que cria a task Celery separado neste arquivo
import os

import gridfs
from ajna_img_functions.models.bsonimage import BsonImageList
from pymongo import MongoClient

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'static')


def trata_bson(bson_file, mongodb_uri, database):
    db = MongoClient(host=mongodb_uri).get_default_database()
    print('Conectou com sucesso!!! ', db.collection_names())
    fs = gridfs.GridFS(db)
    filename = os.path.join(UPLOAD_FOLDER, bson_file)
    bsonimagelist = BsonImageList.fromfile(filename)
    files_ids = bsonimagelist.tomongo(fs)
    return files_ids
