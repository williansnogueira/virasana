"""Funções utilitárias para exportação e montagem de training sets.

"""
from bson import ObjectId
from gridfs import GridFS
from pymongo import MongoClient


from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from virasana.integracao.padma import recorta_imagem

def campos_mongo_para_lista(filtro: str, chaves: list)-> list:
    """Consulta MongoDB retornando lista.

    """
    db = MongoClient(host=MONGODB_URI)[DATABASE]
    cursor = db['fs.files'].find(filtro)
    lista = []
    cabecalhos = []
    for campo in chaves:
        cabecalhos.append(campo)
    lista.append(cabecalhos)
    for linha in cursor:
        registro = []
        for campo in chaves:
            caminho = campo.split('.')
            sub = linha[caminho[0]]
            for chave in caminho[1:]:
                if isinstance(sub, list):
                    sub = sub[0]
                if sub and isinstance(sub, dict):
                    sub = sub.get(chave)
            registro.append(sub)
        lista.append(registro)
    return lista


def get_imagens_recortadas(_id):
    images = []
    db = MongoClient(host=MONGODB_URI)[DATABASE]
    fs = GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    image = grid_data.read()
    preds = grid_data.metadata.get('predictions')
    if preds:
        for pred in preds:
            bbox = pred.get('bbox')
            if bbox:
                try:
                    image = recorta_imagem(image, bbox, True)
                    images.append(image)
                except:
                    print(image)
                    pass
    return images