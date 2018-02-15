from datetime import datetime
import json
import os
import time
from base64 import b64encode, decodebytes

import gridfs
from celery import Celery, states
from flask import (Flask, flash, jsonify, redirect, render_template, request,
                   Response, url_for)
from flask_bootstrap import Bootstrap
from flask_login import current_user, login_required
# from werkzeug.utils import secure_filename
# from flask_cors import CORS
from flask_nav import Nav
from flask_nav.elements import Navbar, View
from flask_wtf.csrf import CSRFProtect
from pymongo import MongoClient

from ajna_commons.models.bsonimage import BsonImageList
from virasana.conf import (BACKEND, BROKER, BSON_REDIS, DATABASE, MONGODB_URI,
                           TIMEOUT, redisdb)

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


celery = Celery(app.name, broker=BROKER,
                backend=BACKEND)


@celery.task(bind=True)
def raspa_dir(self):
    """Background task that go to directory of incoming files
    AND load then to mongodb
    """
    self.update_state(state=states.STARTED,
                      meta={'current': '',
                            'status': 'Iniciando'})
    q = redisdb.lpop(BSON_REDIS)
    q = json.loads(q.decode('utf-8'))
    file = bytes(q['bson'], encoding='utf-8')
    file = decodebytes(file)
    trata_bson(file, db)
    return {'current': '',
            'status': 'Todos os arquivos processados'}


def trata_bson(bson_file, db):
    # .get_default_database()
    fs = gridfs.GridFS(db)
    bsonimagelist = BsonImageList.fromfile(abson=bson_file)
    files_ids = bsonimagelist.tomongo(fs)
    return files_ids


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
# @csrf.exempt # TODO: put CRSF on tests
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
@csrf.exempt  # TODO: put CRSF on tests ??? Or just use JWT???
def api_upload():
    # initialize the data dictionary that will be returned from the
    # view
    data = {'progress': 'Function predict called'}
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


@app.route('/file')
@login_required
def file():
    fs = gridfs.GridFS(db)
    grid_data = fs.find_one({'filename': '20170701002613003RS_stamp.jpg'})
    return render_template('view_file.html', myfile=grid_data)


@app.route('/image/<_id>')
def image(_id):
    fs = gridfs.GridFS(db)
    grid_data = fs.find_one({'filename': '20170701002613003RS_stamp.jpg'})
    image = grid_data.read()
    # image = fs.get(_id).read()
    return Response(response=image, mimetype='image/jpeg')


@app.route('/files')
@login_required
def files(page=1):
    fs = gridfs.GridFS(db)
    lista_arquivos = []
    for param, value in request.args.items():
        print(param, value)
    start = datetime(2017, 1, 1, 0, 0)
    end = datetime(2018, 2, 28, 23, 59)
    print(start, end)
    for grid_data in fs.find({'uploadDate':
                              {'$lt': end,
                               '$gt': start},
                              'metadata.numeroinformado':
                              {'$regex': '^APZU3890'}}
                             ).sort('uploadDate', -1).limit(10):
        linha = {}
        linha['_id'] = grid_data._id
        linha['filename'] = grid_data.filename
        linha['upload_date'] = grid_data.upload_date
        linha['metadata'] = grid_data.metadata
        lista_arquivos.append(linha)
    print(lista_arquivos)
    return render_template('search_files.html', paginated_files=lista_arquivos)


@nav.navigation()
def mynavbar():
    items = [View('Home', 'index'),
             View('Importar Bson', 'upload_bson'),
             View('Pesquisar arquivos', 'files'),
             ]
    if current_user.is_authenticated:
        items.append(View('Sair', 'logout'))
    return Navbar(*items)


SECRET = 'secret'
app.secret_key = SECRET
app.config['SECRET_KEY'] = SECRET
# app.config['SESSION_TYPE'] = 'filesystem'
# Session(app)


nav.init_app(app)


if __name__ == '__main__':
    # start the web server
    print('* Starting web service...')
    app.run()
