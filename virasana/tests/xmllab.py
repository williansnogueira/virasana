"""
Testes interativos em arquivos XML gerados pelos equipamentos.
"""
from datetime import datetime
from pymongo import MongoClient
from gridfs import GridFS
from virasana.workers.xml_functions import dados_xml_grava_fsfiles
# , xml_todict

db = MongoClient()['test']
fs = GridFS(db)

number = db['fs.files'].find(
    {'metadata.xml': None,
     'metadata.contentType': 'image/jpeg'
     }).count()
print(number, 'registros sem metadata de xml')

dados_xml_grava_fsfiles(db, 10000, datetime(1900, 1, 1), True)

print(number, 'registros sem metadata de xml')

"""
filename = '.*xml'
file_cursor = db['fs.files'].find(
    {'filename': {'$regex': filename}}).limit(100)
for linha in file_cursor:
    file_id = linha.get('_id')
    # print(file_id)
    if fs.exists(file_id):
        grid_out = fs.get(file_id)
        xml = grid_out.read().decode('latin1')
        # xml = xml.encode('latin-1').decode('utf-8')
        # TODO: see why sometimes a weird character appears in front of content
        # print(xml)
        posi = xml.find('<DataForm>')
        if posi == -1:
            xml = xml[2:]
        else:
            xml = xml[posi:]
        # print('posi', posi)
        print(xml_todict(xml))
"""
