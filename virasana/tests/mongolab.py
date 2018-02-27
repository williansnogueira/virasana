"""
For tests with PyMongo and MongoDB.

Tests for sintax and operations before putting into main code

"""
from pymongo import MongoClient

db = MongoClient()['test']

# CARGA - teste para checar como pegar Info do Contêiner na Base CARGA


def mongo_find_in(db, collection: str, field: str, in_set: list,
                  set_field: str=None)->(dict, set):
    """Perform a find $in in_set on db.collection.

    Args:
        db: connection to mongo with database setted
        collection: name of mongo collection to query
        field: field to filter by
        in_set: list of values to pass to $in operator
        result_field: field to retrieve unique values (optional)

    Returns:
        dictionary of results, formated key:value (Only not null fields)
        set of set_field

    """
    result = []
    field_set = set()
    cursor = db[collection].find({field: {'$in': list(in_set)}})
    for linha in cursor:
        result.append(
            {str(key): field for key, field in linha.items()
             if field is not None})
        if set_field:
            field_set.add(linha[set_field])
    print(result)
    print(field_set)
    return result, field_set


container = 'tclu2967718'
containeres, conhecimentos_set = manifestos, manifestos_set = mongo_find_in(
    db, 'CARGA.Container', 'container', [container], 'conhecimento')
conhecimentos, _ = mongo_find_in(
    db, 'CARGA.Conhecimento', 'conhecimento', manifestos_set)
manifestos, manifestos_set = mongo_find_in(
    db, 'CARGA.ManifestoConhecimento', 'conhecimento',
    conhecimentos_set, 'manifesto')
escalas, escalas_set = mongo_find_in(
    db, 'CARGA.EscalaManifesto', 'manifesto', manifestos_set, 'escala')
atracacoes, _ = mongo_find_in(
    db, 'CARGA.AtracDesatracEscala', 'escala', escalas_set)

file_cursor = db['fs.files'].find({'metadata.CARGA': None})

count = file_cursor.count()

print('Files count with no CARGA metadata', count)
