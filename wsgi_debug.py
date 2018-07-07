import os
from ajna_commons.flask.conf import VIRASANA_URL

os.environ['DEBUG'] = '1'
from virasana.main import app

if __name__ == '__main__':
    port = 5000
    if VIRASANA_URL:
        port = int(VIRASANA_URL.split(':')[-1])
    app.run(port=port)
