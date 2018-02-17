import os
import pickle
import tempfile

import redis
from ajna_commons.flask.conf import SECRET, tmpdir

BSON_REDIS = 'bson'
REDIS_URL = os.environ.get('REDIS_URL')
if not REDIS_URL:
    REDIS_URL = 'redis://localhost:6379'
BACKEND = BROKER = REDIS_URL
redisdb = redis.StrictRedis.from_url(REDIS_URL)

MONGODB_URI = os.environ.get('MONGODB_URI')
if MONGODB_URI:
    DATABASE = ''.join(MONGODB_URI.rsplit('/')[-1:])
    # print(DATABASE)
else:
    DATABASE = 'test'

# initialize constants used for server queuing
TIMEOUT = 10
BATCH_SIZE = 1000
ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])
