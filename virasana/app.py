import os
import time

from celery import Celery, states
from flask import (Flask, flash, jsonify, request, redirect,
                   render_template, url_for)
from flask_bootstrap import Bootstrap
# from flask_cors import CORS
from flask_nav import Nav
from flask_nav.elements import Navbar, View
from flask_session import Session
# from flask_wtf import FlaskForm
from flask_wtf.csrf import CSRFProtect
from werkzeug.utils import secure_filename

from virasana.workers.raspadir import trata_bson

# initialize constants used for server queuing
TIMEOUT = 10
BATCH_SIZE = 1000
ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

app = Flask(__name__, static_url_path='/static')
app.config['DEBUG'] = True
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# CORS(app)
csrf = CSRFProtect(app)
Bootstrap(app)
nav = Nav()
# logo = img(src='/static/css/images/logo.png')

# TODO: put in separate file
BACKEND = BROKER = 'redis://localhost:6379'

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
    for file in os.listdir(UPLOAD_FOLDER):
        self.update_state(state=states.STARTED,
                          meta={'current': file,
                                'status': 'Processando arquivos...'})
        if 'bson' in file:
            trata_bson(file)
            os.remove(os.path.join(UPLOAD_FOLDER, file))
    return {'current': '',
            'status': 'Todos os arquivos processados'}
######################


def allowed_file(filename):
    """Check allowed extensions"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ['bson', 'bson.zip']


@app.route('/')
def index():
    if True:  # current_user.is_authenticated:
        return render_template('index.html')
    else:
        return redirect(url_for('login'))


@app.route('/uploadbson', methods=['GET', 'POST'])
# @csrf.exempt # TODO: put CRSF on tests 
# @login_required
def upload_bson():
    """Função simplificada para upload do arquivo de uma extração
    """
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            task = raspa_dir.delay()
            # return redirect(url_for('list_files'))
    return render_template('importa_bson.html')


@app.route('/api/uploadbson', methods=['POST'])
@csrf.exempt # TODO: put CRSF on tests ??? Or just use JWT???
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
            filename = secure_filename(file.filename)
            file.save(os.path.join(UPLOAD_FOLDER, filename))
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
# @login_required
def raspadir_progress():
    """Returns a json of raspadir celery task progress"""
    # See where to put task_id (Session???)
    pass


@app.route('/list_files')
# @login_required
def list_files():
    """Lista arquivos no banco MongoDB
    """
    lista_arquivos = []
    return render_template('importa_bson.html', lista_arquivos=lista_arquivos)


@nav.navigation()
def mynavbar():
    items = [View('Home', 'index'),
             View('Importar Bson', 'upload_bson'),
             ]
    return Navbar(*items)

SECRET = 'secret'
app.secret_key = SECRET
app.config['SECRET_KEY'] = SECRET


nav.init_app(app)


if __name__ == '__main__':
    # start the web server
    print('* Starting web service...')
    app.run()
