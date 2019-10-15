from pymongo import MongoClient

from ajna_commons.flask.conf import DATABASE, MONGODB_URI

conn = MongoClient(host=MONGODB_URI)
mongodb = conn[DATABASE]

cont = 0
conterros = 0
for grid_data in mongodb['fs.files'].find(
    {'metadata.carga.ncm': {'$ne': None}},
    {'metadata.carga': 1}
    ):
    _id = grid_data.get('_id')
    metadata = grid_data.get('metadata')
    carga = metadata.get('carga')
    conteiner = carga.get('container')
    if isinstance(conteiner, list):
        conteiner = conteiner[0]
    item = conteiner['item']
    ncms_antigo = carga.get('ncm')
    ncms = [ncm for ncm in carga.get('ncm') if ncm['item'] == item]
    if len(ncms) == 0:
        conterros+=1
        continue
    if len(ncms) > 0 and len(ncms) < len(ncms_antigo):
        # print('Antigo: %s' % carga.get('ncm'))
        # print('Novo: %s' % ncms)
        cont+=1
        mongodb['fs.files'].update_one(
            {'_id': _id},
            {'$set': {'metadata.carga.ncm': ncms}})
print('%d registros atualizados' % cont)
print('%d registros com erro' % conterros)
