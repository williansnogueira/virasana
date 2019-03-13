"""
Testes interativos em arquivos XML gerados pelos escâneres.

Passar um filtro para visualizar um XML

"""
from datetime import datetime

import chardet
import click
from bson.objectid import ObjectId
from gridfs import GridFS

from virasana.db import mongodb as db
from virasana.integracao import xmli

BATCH_SIZE = 50000
today = datetime.today()


@click.command()
@click.option('--year', help='Ano')
@click.option('--month', help='Mes')
@click.option('--terminal', help='Nome do terminal')
@click.option('--arquivo', help='Nome do arquivo')
@click.option('--id', help='ID do arquivo')
def consulta(year, month, terminal, arquivo, id):
    """Script de linha de comando para consulta do arquivo XML."""
    fs = GridFS(db)
    _ids = []
    if id:
        _ids.append(ObjectId(id))
    else:
        filtro = {'metadata.contentType': 'text/xml'}
        if year and month:
            data_inicio = datetime(year, month, 1)
            filtro['metadata.dataescaneamento'] = {'$gt': data_inicio}
        if terminal:
            filtro['metadata.xml.recinto'] = terminal
        if terminal:
            filtro['filename'] = {'$regex': arquivo}
        cursor = db['fs.files'].find(filtro).limit(10)
        _ids = [row['_id'] for row in cursor]
    for _id in _ids:
        if fs.exists(_id):
            grid_out = fs.get(_id)
            raw = grid_out.read()
            encode = chardet.detect(raw)
            try:
                xml = raw.decode(encode['encoding'])
                print(xml)
                print(xmli.xml_todict(xml))
            except Exception as err:
                print(err)


if __name__ == '__main__':
    consulta()
