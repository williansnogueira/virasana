import numpy as np
import os
from datetime import datetime, timedelta

from bson.objectid import ObjectId
from sklearn.metrics.pairwise import euclidean_distances


IMAGE_INDEXES = os.path.join(os.path.dirname(__file__), 'indexes.npy')
IDS_INDEXES = os.path.join(os.path.dirname(__file__), '_ids.npy')


class ImageSearch():
    def __init__(self, db, chunk=20):
        self.db = db
        self.image_indexes = np.load(IMAGE_INDEXES)
        self.ids_indexes = np.load(IDS_INDEXES)
        self.chunk = chunk
        self.cache = {}

    def get_distances(self, search_index):
        distances = euclidean_distances([search_index], self.image_indexes)
        sequence = np.argsort(distances)
        return sequence.reshape(-1)[:1000]  # Economiza memória

    def get_cache(self, _id):
        cache = self.cache.get(_id)
        if not cache or cache.get('expires') > datetime.now():
            return self.search(_id)
        return cache

    def search(self, _id):
        grid_data = self.db['fs.files'].find_one({'_id': ObjectId(_id)})
        preds = grid_data.get('metadata').get('predictions')
        if not preds:
            raise KeyError('Imagem %d não tem índice de busca' % _id)
        indexes = [pred.get('index') for pred in preds]
        if len(indexes) > 0 and indexes[0]:
            search_index = preds[0].get('index')
        seq = self.get_distances(search_index)
        data_id = {
            'seq': seq,
            'expires': datetime.now() + timedelta(minutes=30)
        }
        self.cache[_id] = data_id
        return self.cache[_id]

    def get_chunk(self, _id, offset):
        cache = self.get_cache(_id)
        start = offset * self.chunk
        end = start + 40
        seq = cache['seq']
        seq = seq[start:end]
        most_similar = [str(self.ids_indexes[ind]) for ind in seq]
        return most_similar
