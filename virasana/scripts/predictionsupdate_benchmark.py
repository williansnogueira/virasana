"""Script de linha de comando para integração do Sistema PADMA.

Testes de desempenho do Servidor em diferentes cenários

"""
import asyncio
import time

from ajna_commons.utils.images import mongo_image
from virasana.views import db

from predictionsupdate import (fazconsulta,
                               get_images, monta_filtro)


s0 = time.time()
# Monta lista com 50 imagens recortadas
modelo = 'peso'
filtro = monta_filtro(modelo, sovazios=False, update=True)
cursor = db['fs.files'].find(filtro, {'metadata.predictions': 1}).limit(500)
imagens_recortadas = []
for registro in cursor:
    _id = registro['_id']
    pred_gravado = registro.get('metadata').get('predictions')
    image = mongo_image(db, _id)
    imagens_recortadas.extend(get_images(model=modelo, _id=_id, image=image,
                                         predictions=pred_gravado))

print(len(imagens_recortadas))


loop = asyncio.get_event_loop()
index = 0
tempos = {}
for lote in [1, 10, 20, 40, 50, 60, 80, 100]:
    index += lote
    img_lote = imagens_recortadas[index: index + lote]
    s0 = time.time()
    loop.run_until_complete(fazconsulta(img_lote, modelo))
    s1 = time.time()
    tempos[lote] = (s1 - s0) / lote


for key, value in tempos.items():
    print('Tempo total de execução em segundos por imagem: {0:.3f}'
          .format(value) + ' -- %d imagens.' % key)

s1 = time.time()
print('Tempo total de execução em segundos: {0:.2f}'.format(s1 - s0))
