"""
Módulo de entrada da aplicação web.

"""
from flask import abort, redirect, render_template, request, url_for
from flask_login import login_required, login_user, logout_user

from ajna_commons.flask.login import (DBUser, authenticate, is_safe_url,
                                      login_manager)
from virasana.views import app, db

login_manager.init_app(app)
DBUser.dbsession = db


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('senha')
        registered_user = authenticate(username, password)
        if registered_user is not None:
            print('Logged in..')
            print(login_user(registered_user))
            # print('Current user ', current_user)
            next = request.args.get('next')
            if not is_safe_url(next):
                return abort(400)
            return redirect(next or url_for('index'))
        else:
            return abort(401)
    else:
        return render_template('login.html', form=request.form)


@app.route('/logout')
@login_required
def logout():
    logout_user()
    next = request.args.get('next')
    if not is_safe_url(next):
        next = None
    return redirect(next or url_for('index'))


if __name__ == '__main__':
    app.run(debug=app.config['DEBUG'])
