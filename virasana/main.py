"""Módulo de entrada da aplicação web."""

import ajna_commons.flask.log as log
from virasana.db import mongodb
from virasana.views import configure_app
from ajna_commons.flask.flask_log import configure_applog

app = configure_app(mongodb)
configure_applog(app)
log.logger.info('Servidor (re)iniciado!')

if __name__ == '__main__':  # pragma: no cover
    app.run(debug=app.config['DEBUG'])
