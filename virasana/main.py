"""Módulo de entrada da aplicação web."""
from pymongo import MongoClient

import ajna_commons.flask.log as log
from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from virasana.views import configure_app
from ajna_commons.flask.flask_log import configure_applog

conn = MongoClient(host=MONGODB_URI)
mongodb = conn[DATABASE]
app = configure_app(mongodb)
configure_applog(app)
log.logger.info('Servidor (re)iniciado!')

if __name__ == '__main__':  # pragma: no cover
    app.run(debug=app.config['DEBUG'])
