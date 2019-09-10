import json
import pymongo
from bson import ObjectId


def update_due(db, dues):
    # print(dues)
    for _id, due in dues.items():
        print('Updating %s ' % _id)
        print('with %s ' % json.dumps(due)[:50])
        result = db.fs.files.update_one(
            {'_id': ObjectId(_id)},
            {'$set': {'metadata.due': due}}
        )
        print(result)


CHAVES_DUE = [
    'metadata.due.numero',
    'metadata.due.ruc',
    'metadata.due.Declarante',
    'metadata.due.PaisImportador',
    'metadata.due.itens.Exportador',
    'metadata.due.itens.recintoAduaneiroDespacho.codigo',
    'metadata.due.itens.recintoAduaneiroEmbarque.codigo',
]


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração.

    São criados índices para desempenho nas consultas.
    Alguns índices únicos também são criados, estes para evitar importação
    duplicada do mesmo registro.
    """
    for campo in CHAVES_DUE:
        try:
            db['fs.files'].create_index(campo)
        except pymongo.errors.OperationFailure:
            pass



if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para DUE')
    create_indexes(db)
