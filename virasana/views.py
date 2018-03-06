import json
import os
from base64 import b64encode
from datetime import date, datetime, timedelta

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
from flask_wtf.csrf import CSRFProtect
from pymongo import MongoClient
from wtforms import DateField, StringField
from wtforms.validators import optional

from ajna_commons.flask.conf import (BSON_REDIS, DATABASE, MONGODB_URI, SECRET,
                                     redisdb)

from ajna_commons.flask.log import logger

from virasana.workers.tasks import raspa_dir

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
    # print(current_user)
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
        file = request.files.get('file')
        if not file or file.filename == '' or not allowed_file(file.filename):
            flash('Arquivo não informado ou inválido!')
            return redirect(request.url)
        content = file.read()
        d = {'bson': b64encode(content).decode('utf-8')}
        redisdb.rpush(BSON_REDIS, json.dumps(d))
        result = raspa_dir.delay()
    return redirect(url_for('list_files', taskid=result.id))


@app.route('/api/uploadbson', methods=['POST'])
@csrf.exempt  # TODO: put CSRF on tests ??? Or just use JWT???
def api_upload():
    """Função para upload via API de um arquivo BSON.

    Coloca o arquivo numa queue do Banco de Dados Redis
    e inicia uma task Celery. O resultado do processamento efetivo do 
    arquivo pode ser acompanhado na view 
    py:func:`task_progress`

    Args:
        file: arquivo BSON gerado pelo AJNA e enviado via HTTP POST
    Returns:
        json['success']: True or False
        json['taskid']: ID da task do celery iniciada para 

    """
    # ensure a bson was properly uploaded to our endpoint
    file = request.files.get('file')
    data = {'success': False,
            'mensagem': 'Task iniciada',
            'taskid': 0}
    try:
        if not file or file.filename == '' or not allowed_file(file.filename):
            if not file:
                data['mensagem'] = 'Arquivo nao informado'
            elif not file.filename:
                data['mensagem'] = 'Nome do arquivo vazio'
            else:
                data['mensagem'] = 'Nome de arquivo não permitido: ' + file.filename
            print(file)
        else:
            content = file.read()
            d = {'bson': b64encode(content).decode('utf-8'),
                 'filename': file.filename}
            redisdb.rpush(BSON_REDIS, json.dumps(d))
            result = raspa_dir.delay()
            data['taskid'] = result.id
            data['success'] = True
    except Exception as err:
        logger.error(err, exc_info=True)
        data['mensagem'] = 'Excecao ' + str(err)

    return jsonify(data)


@app.route('/api/task/<taskid>')
# @login_required
def task_progress(taskid):
    """Returns a json of celery task progress."""
    task = raspa_dir.AsyncResult(taskid)
    response = {
        'state': task.state,
        'current': task.info.get('current', ''),
        'status': task.info.get('status', '')
    }
    return jsonify(response)


@app.route('/list_files')
@login_required
def list_files():
    """Lista arquivos no banco MongoDB
    """
    fs = gridfs.GridFS(db)
    lista_arquivos = []
    for grid_data in fs.find().sort('uploadDate', -1).limit(10):
        lista_arquivos.append(grid_data.filename)
    # print(lista_arquivos)
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
    # print(grid_data)
    return render_template('view_file.html', myfile=grid_data)


@app.route('/image/<_id>')
def image(_id):
    fs = gridfs.GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    image = grid_data.read()
    return Response(response=image, mimetype='image/jpeg')


class FilesForm(FlaskForm):
    numero = StringField('Número', validators=[optional()])
    start = DateField('Start', validators=[optional()],
                      default=date.today() - timedelta(days=90))
    end = DateField('End', validators=[optional()], default=date.today())


@app.route('/files', methods=['GET', 'POST'])
@login_required
def files(page=1):
    fs = gridfs.GridFS(db)
    lista_arquivos = []
    form = FilesForm(**request.form)
    if form.validate():
        numero = form.numero.data
        start = form.start.data
        end = form.end.data
        if numero == 'None':
            numero = None
        filtro = {}
        if start and end:
            start = datetime.combine(start, datetime.min.time())
            end = datetime.combine(end, datetime.min.time())
            filtro['metadata.dataescaneamento'] = {'$lt': end, '$gt': start}
        if numero:
            filtro['metadata.numeroinformado'] = {'$regex': numero}
        # print(filtro)
        for grid_data in fs.find(filtro).sort('uploadDate', -1).limit(10):
            linha = {}
            linha['_id'] = grid_data._id
            linha['filename'] = grid_data.filename
            linha['upload_date'] = grid_data.metadata.get('dataescaneamento')
            linha['numero'] = grid_data.metadata.get('numeroinformado')
            lista_arquivos.append(linha)
        # print(lista_arquivos)
    return render_template('search_files.html',
                           paginated_files=lista_arquivos,
                           oform=form)


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
