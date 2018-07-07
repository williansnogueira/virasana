"""Módulo de entrada da aplicação web."""
from pymongo import MongoClient

from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from virasana.views import configure_app

conn = MongoClient(host=MONGODB_URI)
mongodb = conn[DATABASE]
app = configure_app(mongodb)

if __name__ == '__main__':  # pragma: no cover
    app.run(debug=app.config['DEBUG'])
