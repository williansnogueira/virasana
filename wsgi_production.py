import logging
from werkzeug.wsgi import DispatcherMiddleware

from virasana.main import app

application = DispatcherMiddleware(app,
                                   {
                                       '/virasana': app
                                   })
if __name__ == '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)