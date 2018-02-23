import os
os.environ['DEBUG'] = '1'

from ajna_commons.flask.conf import VIRASANA_URL
from virasana.app import app

if __name__ == '__main__':
    port = 5000
    if VIRASANA_URL:
        port = int(VIRASANA_URL.split(':')[-1])
    app.run(port=port)