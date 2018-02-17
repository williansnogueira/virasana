import json
import os
import time
from base64 import b64encode
from datetime import datetime, timedelta

import gridfs
from bson.objectid import ObjectId
from flask import (Flask, Response, flash, jsonify, redirect, render_template,
                   request, url_for)
from flask_bootstrap import Bootstrap
from flask_login import current_user, login_required
# from werkzeug.utils import secure_filename
# from flask_cors import CORS
from flask_nav import Nav
from flask_nav.elements import Navbar, View
from flask_wtf import FlaskForm
from wtforms import DateField, DateTimeField, SubmitField, StringField
from wtforms.validators import DataRequired, optional
from flask_wtf.csrf import CSRFProtect
from pymongo import MongoClient

from celery import states
from virasana.conf import (BSON_REDIS, DATABASE, MONGODB_URI, SECRET, TIMEOUT,
                           redisdb)
from virasana.workers.raspadir import raspa_dir

app = Flask(__name__, static_url_path='/static')
app.config['DEBUG'] = True
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# CORS(app)
db = MongoClient(host=MONGODB_URI)[DATABASE]
csrf = CSRFProtect(app)
Bootstrap(app)
nav = Nav()
# logo = img(src='/static/css/images/logo.png')


def allowed_file(filename):
    """Check allowed extensions"""
    return '.' in filename and \
        filename.rsplit('.', 1)[-1].lower() in ['bson']


@app.route('/')
def index():
    print(current_user)
    if current_user.is_authenticated:
        return render_template('index.html')
    else:
        return redirect(url_for('login'))


@app.route('/uploadbson', methods=['GET', 'POST'])
# @csrf.exempt # TODO: put CSRF on tests
@login_required
def upload_bson():
    """Função simplificada para upload do arquivo de uma extração
    """
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files.get('file')
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            content = file.read()
            d = {'bson': b64encode(content).decode('utf-8')}
            redisdb.rpush(BSON_REDIS, json.dumps(d))
            raspa_dir.delay()
    return redirect(url_for('list_files'))


@app.route('/api/uploadbson', methods=['POST'])
@csrf.exempt  # TODO: put CSRF on tests ??? Or just use JWT???
def api_upload():
    # initialize the data dictionary that will be returned from the
    # view
    data = {'progress': 'Function called'}
    s0 = None
    # ensure a bson was properly uploaded to our endpoint
    if request.method == 'POST':
        data['progress'] = 'Post checked'
        file = request.files.get('file')
        if file and file.filename != '' and allowed_file(file.filename):
            data['progress'] = 'File checked'
            s0 = time.time()
            print('Enter Sandman - sending request to celery queue')
            content = file.read()
            d = {'bson': b64encode(content).decode('utf-8')}
            redisdb.rpush(BSON_REDIS, json.dumps(d))
            data['progress'] = 'File uploaded'
            task = raspa_dir.delay()
            data['progress'] = 'Task initiated'
            # Wait 10s for celery to return success or failure
            for r in range(1, 50):
                time.sleep(TIMEOUT / 50)
                if task.state in states.READY_STATES:
                    data['progress'] = 'Task ended'
                    break
            if task.state not in states.READY_STATES:
                data['progress'] = (
                    'Timeout! Checar se serviço Celery está '
                    'rodando e se não está travado. A tarefa pode '
                    ' estar também demorando muito tempo para executar. \n '
                    'task celery raspa_dir \n '
                    'Timeout configurado para ' + str(TIMEOUT) + 's')

    # return the data dictionary as a JSON response
    if s0 is not None:
        s1 = time.time()
        print(s1, 'Results read from queue and returned in ', s1 - s0)
    if task and task.info:
        data['state'] = task.state
        data = {**data, **task.info}
    return jsonify(data)


@app.route('/raspadir_progress')
@login_required
def raspadir_progress():
    """Returns a json of raspadir celery task progress"""
    # See where to put task_id (Session???)
    pass


@app.route('/list_files')
@login_required
def list_files():
    """Lista arquivos no banco MongoDB
    """
    fs = gridfs.GridFS(db)
    lista_arquivos = []
    for grid_data in fs.find().sort('uploadDate', -1).limit(10):
        lista_arquivos.append(grid_data.filename)
    print(lista_arquivos)
    return render_template('importa_bson.html', lista_arquivos=lista_arquivos)


@app.route('/file/<_id>')
@app.route('/file')
@login_required
def file(_id=None):
    fs = gridfs.GridFS(db)
    if request.args.get('filename'):
        grid_data = fs.find_one({'filename': request.args.get('filename')})
    else:
        grid_data = fs.get(ObjectId(_id))
    return render_template('view_file.html', myfile=grid_data)


@app.route('/image/<_id>')
def image(_id):
    fs = gridfs.GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    image = grid_data.read()
    return Response(response=image, mimetype='image/jpeg')


class FilesForm(FlaskForm):
    numero = StringField('Número', validators=[DataRequired()])
    start = DateField('Start', validators=[optional()], default=datetime.utcnow() - timedelta(days=90))
    end = DateField('End', validators=[optional()], default=datetime.utcnow())


@app.route('/files', methods=['GET', 'POST'])
@login_required
def files(page=1):
    fs = gridfs.GridFS(db)
    lista_arquivos = []
    form = FilesForm(**request.form)
    numero = form.numero.data
    start = form.start.data
    end = form.end.data
    print(numero, start, end)
    if form.validate():
        for grid_data in fs.find({'uploadDate':
                                  {'$lt': end,
                                   '$gt': start},
                                  'metadata.numeroinformado':
                                  {'$regex': '^' + numero}}
                                 ).sort('uploadDate', -1).limit(10):
            linha = {}
            linha['_id'] = grid_data._id
            linha['filename'] = grid_data.filename
            linha['upload_date'] = grid_data.upload_date
            linha['metadata'] = grid_data.metadata
            lista_arquivos.append(linha)
        print(lista_arquivos)
        return render_template('search_files.html',
                               paginated_files=lista_arquivos,
                               numero=numero)
    return render_template('search_files.html', numero=numero, start=start, end=end)


@nav.navigation()
def mynavbar():
    items = [View('Home', 'index'),
             View('Importar Bson', 'upload_bson'),
             View('Pesquisar arquivos', 'files'),
             ]
    if current_user.is_authenticated:
        items.append(View('Sair', 'logout'))
    return Navbar(*items)


app.config['DEBUG'] = os.environ.get('DEBUG', 'None') == '1'
if app.config['DEBUG'] is True:
    app.jinja_env.auto_reload = True
    app.config['TEMPLATES_AUTO_RELOAD'] = True
app.secret_key = SECRET
app.config['SECRET_KEY'] = SECRET
# app.config['SESSION_TYPE'] = 'filesystem'
# Session(app)

nav.init_app(app)

if __name__ == '__main__':
    # start the web server
    print('* Starting web service...')
    app.run(debug=app.config['DEBUG'])
