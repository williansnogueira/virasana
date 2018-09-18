import os
from werkzeug.wsgi import DispatcherMiddleware

from virasana.main import app

application = DispatcherMiddleware(app,
                                   {
                                       '/virasana': app
                                   })
