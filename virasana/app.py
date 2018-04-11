"""Módulo de entrada da aplicação web."""
import ajna_commons.flask.login as login
from virasana.views import app, db

login.login_manager.init_app(app)
login.configure(app)
login.DBUser.dbsession = db

if __name__ == '__main__':
    app.run(debug=app.config['DEBUG'])
