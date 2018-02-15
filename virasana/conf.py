import os
import pickle
import redis
import tempfile

BSON_REDIS = 'bson'
REDIS_URL = os.environ.get('REDIS_URL')
if not REDIS_URL:
    REDIS_URL = 'redis://localhost:6379'
BACKEND = BROKER = REDIS_URL
redisdb = redis.StrictRedis.from_url(REDIS_URL)

MONGODB_URI = os.environ.get('MONGODB_URI')
if MONGODB_URI:
    DATABASE = ''.join(MONGODB_URI.rsplit('/')[-1:])
    print(DATABASE)
else:
    DATABASE = 'test'

# initialize constants used for server queuing
TIMEOUT = 10
BATCH_SIZE = 1000
ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])


tmpdir = tempfile.mkdtemp()


try:
    SECRET = None
    with open('SECRET', 'rb') as secret:
        try:
            SECRET = pickle.load(secret)
        except pickle.PickleError:
            pass
except FileNotFoundError:
    pass

if not SECRET:
    SECRET = os.urandom(24)
    with open('SECRET', 'wb') as out:
        pickle.dump(SECRET, out, pickle.HIGHEST_PROTOCOL)
