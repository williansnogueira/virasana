"""Módulo de entrada da aplicação web."""
from flask import abort, redirect, render_template, request, url_for
from flask_login import login_required, login_user, logout_user
# from urllib.parse import urlparse, urljoin
from ajna_commons.flask.login import (DBUser, authenticate, is_safe_url,
                                      login_manager)
from virasana.views import app, db

login_manager.init_app(app)
DBUser.dbsession = db

"""
def is_safe_url(target):
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and \
           ref_url.netloc == test_url.netloc
"""


@app.route('/login', methods=['GET', 'POST'])
def login():
    """View para efetuar login."""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('senha')
        registered_user = authenticate(username, password)
        if registered_user is not None:
            print('Logged in..')
            print(login_user(registered_user))
            # print('Current user ', current_user)
            next_url = request.args.get('next')
            if not is_safe_url(next_url):
                return abort(400)
            return redirect(next_url or url_for('index'))
        else:
            return abort(401)
    else:
        return render_template('login.html', form=request.form)


@app.route('/logout')
@login_required
def logout():
    """View para efetuar logout."""
    logout_user()
    next = request.args.get('next')
    if not is_safe_url(next):
        next = None
    return redirect(next or url_for('index'))


if __name__ == '__main__':
    app.run(debug=app.config['DEBUG'])
