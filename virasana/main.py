"""Módulo de entrada da aplicação web do módulo Virasana.

Módulo Virasana é o Servidor de imagens e a interface para carga,
consulta e integração das imagens com outras bases.

"""

import ajna_commons.flask.log as log
from ajna_commons.flask.flask_log import configure_applog
from virasana.db import mongodb
from virasana.views import configure_app

app = configure_app(mongodb)
configure_applog(app)
log.logger.info('Servidor (re)iniciado!')

if __name__ == '__main__':  # pragma: no cover
    app.run(debug=app.config['DEBUG'])
