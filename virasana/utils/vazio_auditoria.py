from virasana.views import consulta_padma, db


cursor = db['fs.files'].find({'metadata.carga.vazio': True}, {'_id': 1})

for r in range(10):
    registro = next(cursor)
    print(registro['_id'])
    consulta_padma(registro['_id'])




