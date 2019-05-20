"""
Script que pega os índices já gerados e coloca em array numpy
"""
import os

import numpy as np

from virasana.db import mongodb as db

VIRASANA_MODELS = os.path.join('virasana', 'models')


def gera_indexes():
    cursor = db['fs.files'].find(
        {'metadata.predictions.index': {'$exists': True, '$ne': None}},
        {'metadata.predictions.index': 1}
    )

    lista_indexes = []
    lista_ids = []
    for index in cursor:
        lista_indexes.append(index.get('metadata'
                                       ).get('predictions')[0].get('index'))
        lista_ids.append(index.get('_id'))

    np_indexes = np.asarray(lista_indexes, dtype=np.float16)
    np_ids = np.asarray(lista_ids)

    np.save(os.path.join(VIRASANA_UTILS, 'indexes.npy'), np_indexes)
    np.save(os.path.join(VIRASANA_UTILS, '_ids.npy'),
            np.asarray(np_ids))


if __name__ == '__main__':
    gera_indexes()
