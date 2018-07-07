import logging
from virasana.main import app
from ajna_commons.flask.log import error_handler, sentry_handler

if __name__ == '__main__':
    app.logger.addHandler(error_handler)
    if sentry_handler:
        app.logger.addHandler(sentry_handler)
    app.logger.setLevel(logging.INFO)
    app.logger.warning('Servidor (re)iniciado!')
    app.run()