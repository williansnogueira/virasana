from urllib.parse import urljoin, urlparse

from flask import abort, redirect, render_template, request, url_for
from flask_login import (LoginManager, UserMixin, login_required, login_user,
                         logout_user)
from flask_wtf import FlaskForm
from wtforms import BooleanField, PasswordField, StringField, SubmitField
from wtforms.validators import DataRequired, Length

from virasana.views import app

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.session_protection = 'strong'


class DBUser():
    def __init__(self, id):
        self.id = id
        self.name = str(id)

    @classmethod
    def get(cls, session, username, password):
        if username:
            return DBUser(username)
        return None


class User(UserMixin):
    user_database = DBUser

    def __init__(self, id):
        self.id = id
        self.name = str(id)

    @classmethod
    def get(cls, username, password=None):
        dbsession = ''
        dbuser = cls.user_database.get(dbsession, username, password)
        if dbuser:
            print('Nome:', dbuser.name)
            return User(dbuser.name)
        return None


class LoginForm(FlaskForm):
    nome = StringField('Nome', validators=[DataRequired(), Length(1, 50)])
    senha = PasswordField('Senha', validators=[DataRequired()])
    remember_me = BooleanField('Lembrar-me')
    submit = SubmitField('Entrar')


def authenticate(username, password):
    user_entry = User.get(username, password)
    print('User: ', user_entry)
    return user_entry


@login_manager.user_loader
def load_user(userid):
    user_entry = User.get(userid)
    return user_entry


def is_safe_url(target):
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and \
        ref_url.netloc == test_url.netloc


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
