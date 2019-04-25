"""Utiliza np.arrays de hash das imagens para busca distância euclidiana."""
import os
from datetime import datetime, timedelta

import numpy as np
from ajna_commons.flask.log import logger
from bson.objectid import ObjectId
from sklearn.metrics.pairwise import euclidean_distances

from virasana.scripts.gera_indexes import gera_indexes

# Local onde as arrays geradas ficam gravadas
# Deve haver um serviço gerando as arrays a partir do banco de dados
IMAGE_INDEXES = os.path.join(os.path.dirname(__file__), 'indexes.npy')
IDS_INDEXES = os.path.join(os.path.dirname(__file__), '_ids.npy')


class ImageSearch():
    """Buscador de imagens.

    Através das informações de duas arrays paralelas, uma com _ids de imagens e
    outra com hashs das imagens, ordena lista de _ids por similaridade,
    utilizando distância euclidiana entre uma imagem alvo e as demais imagens.

    Uso:
        search = ImageSearch(db, chunk=20)
        search.get_chunk(_id, n) - Retorna lista de n*chunk a (n+1)*chunk
        search.get_list(_id) - Retorna lista completa
    """

    def __init__(self, db=None, chunk=20, cache_size=1000):
        """Recebe conexão ao BD, carrega arrays, inicializa variáveis.

        Args:
            db: conexão MongoDB
            chunk: tamanho da quantidade de ids retornados por get_chunk
            cache_size: tamanho da listagem de ids que será guardada
        """
        self.db = db
        if not os.path.exists(IMAGE_INDEXES):
            logger.info(
                'Criando índices de imagesearch... arquivo utils/indexes.npy')
            gera_indexes()
        self.image_indexes = np.load(IMAGE_INDEXES)
        self.ids_indexes = np.load(IDS_INDEXES)
        logger.info('Índice de %s imagens ' % len(self.image_indexes) +
                    'Para buscas de similaridade carregado em ImageSearch')
        self.chunk = chunk
        self.cache_size = cache_size
        self.cache = {}

    def get_size(self):
        return len(self.image_indexes)

    def get_distances(self, search_index):
        """Retorna lista de cache_size _ids ordenados por similaridade."""
        distances = euclidean_distances([search_index], self.image_indexes)
        sequence = np.argsort(distances)
        return sequence.reshape(-1)[:1000]  # Economiza memória

    def _search_for_index(self, search_index, _id):
        seq = self.get_distances(search_index)
        data_id = {
            'seq': seq,
            'expires': datetime.now() + timedelta(minutes=30)
        }
        self.cache[_id] = data_id
        return self.cache[_id]

    def _search(self, _id):
        """Consulta no banco imagem a buscar. Monta cache de ids similares."""
        if self.db:
            grid_data = self.db['fs.files'].find_one({'_id': ObjectId(_id)})
            preds = grid_data.get('metadata').get('predictions')
            if not preds:
                raise KeyError('Imagem %d não tem índice de busca' % _id)
            indexes = [pred.get('index') for pred in preds]
            if len(indexes) > 0 and indexes[0]:
                search_index = preds[0].get('index')
        else:
            search_index = self.image_indexes[np.where(
                self.ids_indexes == _id)[0]][0]
        return self._search_for_index(search_index, _id)

    def _get_cache(self, _id, index=None):
        """Consulta se há cache. Não havendo, gera."""
        cache = self.cache.get(_id)
        if cache is None or cache.get('expires') < datetime.now():
            if index is not None:
                return self._search_for_index(index, _id)
            return self._search(_id)
        return cache

    def get_chunk(self, _id, offset=0, index=None):
        """Retorna slice da lista de _ids."""
        cache = self._get_cache(_id, index)
        start = offset * self.chunk
        end = start + 40
        seq = cache['seq']
        seq = seq[start:end]
        most_similar = [str(self.ids_indexes[ind]) for ind in seq]
        return most_similar

    def get_list(self, _id, index=None):
        """Retorna slice da lista de _ids."""
        cache = self._get_cache(_id, index)
        seq = cache['seq']
        most_similar = [str(self.ids_indexes[ind]) for ind in seq]
        return most_similar
