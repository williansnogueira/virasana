"""Coleção de views da interface web do módulo virasana."""
import json
import os
from base64 import b64encode
from datetime import date, datetime, timedelta
from sys import platform

import requests
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
from gridfs import GridFS
from pymongo import MongoClient
from wtforms import (BooleanField, DateField, IntegerField, SelectField,
                     StringField)
from wtforms.validators import optional

from ajna_commons.flask.conf import (BSON_REDIS, DATABASE, MONGODB_URI,
                                     PADMA_URL, SECRET, redisdb)
from ajna_commons.flask.log import logger
from ajna_commons.utils.images import mongo_image, recorta_imagem
from virasana.integracao import (CHAVES_GRIDFS, plot_bar, plot_pie,
                                 stats_resumo_imagens)
from virasana.integracao.carga import CHAVES_CARGA
from virasana.workers.tasks import raspa_dir, trata_bson

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
stats_cache = {}


def allowed_file(filename):
    """Checa extensões permitidas."""
    return '.' in filename and \
        filename.rsplit('.', 1)[-1].lower() in ['bson']


@app.route('/')
def index():
    """View retorna index.html ou login se não autenticado."""
    # print(current_user)
    if current_user.is_authenticated:
        return render_template('index.html')
    else:
        return redirect(url_for('login'))


@app.route('/uploadbson', methods=['GET', 'POST'])
@login_required
def upload_bson():
    """Função simplificada para upload do arquivo de uma extração.

    Ver API/Upload BSON
    """
    taskid = ''
    if request.method == 'POST':
        # check if the post request has the file part
        file = request.files.get('file')
        if not file or file.filename == '' or not allowed_file(file.filename):
            flash('Arquivo não informado ou inválido!')
            return redirect(request.url)
        content = file.read()
        if platform == 'win32':
            with MongoClient(host=MONGODB_URI) as conn:
                db = conn[DATABASE]
                trata_bson(content, db)
        else:
            d = {'bson': b64encode(content).decode('utf-8')}
            redisdb.rpush(BSON_REDIS, json.dumps(d))
            result = raspa_dir.delay()
            taskid = result.id
    if taskid:
        return redirect(url_for('list_files', taskid=taskid))
    return redirect(url_for('list_files'))


@app.route('/api/uploadbson', methods=['POST'])
@csrf.exempt
# @login_required
def api_upload():
    """Função para upload via API de um arquivo BSON.

    Coloca o arquivo numa queue do Banco de Dados Redis
    e inicia uma task Celery. O resultado do processamento efetivo do
    arquivo pode ser acompanhado na view
    py:func:`task_progress`

    Args:
        file: arquivo BSON gerado pelo AJNA e enviado via HTTP POST

    Returns:
        json['success']: True ou False
        json['taskid']: ID da task do celery a ser monitorada

    """
    # ensure a bson was properly uploaded to our endpoint
    file = request.files.get('file')
    data = {'success': False,
            'mensagem': 'Task iniciada',
            'taskid': ''}
    try:
        if not file or file.filename == '' or not allowed_file(file.filename):
            if not file:
                data['mensagem'] = 'Arquivo nao informado'
            elif not file.filename:
                data['mensagem'] = 'Nome do arquivo vazio'
            else:
                data['mensagem'] = 'Nome de arquivo não permitido: ' + \
                    file.filename
            print(file)
        else:
            content = file.read()
            if platform == 'win32':
                with MongoClient(host=MONGODB_URI) as conn:
                    db = conn[DATABASE]
                    trata_bson(content, db)
                data['success'] = True
            else:
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
@login_required
def task_progress(taskid):
    """Retorna um json do progresso da celery task."""
    task = raspa_dir.AsyncResult(taskid)
    response = {'state': task.state}
    if task.info:
        response['current'] = task.info.get('current', ''),
        response['status'] = task.info.get('status', '')
    return jsonify(response)


@app.route('/list_files')
@login_required
def list_files():
    """Lista arquivos no banco MongoDB.

    Lista 10 arquivos mais recentes no banco MongoDB,
    por uploadDate mais recente.
    Se houver upload em andamento, informa.
    """
    fs = GridFS(db)
    lista_arquivos = []
    for grid_data in fs.find().sort('uploadDate', -1).limit(10):
        lista_arquivos.append(grid_data.filename)
    taskid = request.args.get('taskid')
    task_info = None
    if taskid:
        task = raspa_dir.AsyncResult(taskid)
        task_info = task.info
    return render_template('importa_bson.html',
                           lista_arquivos=lista_arquivos,
                           task_info=task_info)


@app.route('/file/<_id>')
@app.route('/file')
@login_required
def file(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe o arquivo e os metadados associados a ele.
    """
    fs = GridFS(db)
    if request.args.get('filename'):
        grid_data = fs.find_one({'filename': request.args.get('filename')})
    else:
        grid_data = fs.get(ObjectId(_id))
    # print(grid_data)
    return render_template('view_file.html', myfile=grid_data)


@app.route('/image')
@login_required
def image():
    """Serializa a imagem do banco para stream HTTP."""
    filtro = {key: value for key, value in request.args.items()}
    linha = db['fs.files'].find_one(filtro, {'_id': 1})
    if linha:
        return image_id(linha['_id'])
    return ''


@app.route('/grid_data')
@login_required
def grid_data():
    """Serializa os dados do banco para stream JSON HTTP."""
    filtro = {key: value for key, value in request.args.items()}
    linha = db['fs.files'].find_one(filtro)

    class BSONEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, ObjectId):
                return str(o)
            if isinstance(o, datetime):
                return datetime.strftime(o, '%x %X')
            return json.JSONEncoder.default(self, o)
    if linha:
        return BSONEncoder().encode(linha)


@app.route('/image/<_id>')
@login_required
def image_id(_id):
    """Recorta a imagem do banco e serializa para stream HTTP."""
    image = mongo_image(db, _id)
    if image:
        return Response(response=image, mimetype='image/jpeg')
    return 'Sem Imagem'


@app.route('/mini1/<_id>')
@login_required
def mini(_id, n=0):
    """Recorta a imagem do banco e serializa para stream HTTP."""
    fs = GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    image = grid_data.read()
    preds = grid_data.metadata.get('predictions')
    if preds:
        bboxes = [pred.get('bbox') for pred in preds]
        if len(bboxes) >= n + 1 and bboxes[n]:
            image = recorta_imagem(image, bboxes[n])
            return Response(response=image, mimetype='image/jpeg')
    return 'Sem imagem'


@app.route('/mini2/<_id>')
@login_required
def mini2(_id):
    """Link para imagem do segundo contêiner, se houver."""
    return mini(_id, 1)


filtros = dict()


def campos_chave():
    """Retorna campos chave para montagem de filtro."""
    return CHAVES_GRIDFS + CHAVES_CARGA


@app.route('/filtro_personalizado', methods=['GET', 'POST'])
# @login_required
def filtro():
    """Configura filtro personalizado."""
    user_filtros = filtros[current_user.id]
    print(request.form)
    print(request.args)
    campo = request.args.get('campo')
    if campo:
        valor = request.args.get('valor')
        if valor:   # valor existe, adiciona
            user_filtros[campo] = valor
        else:  # valor não existe, exclui chave
            user_filtros.pop(campo)
    result = [{'campo': k, 'valor': v} for k, v in user_filtros.items()]
    return jsonify(result)


FILTROS_AUDITORIA = {
    '1': {'filtro': {'metadata.carga.vazio': True,
                     'metadata.predictions.vazio': False},
          'order': [('metadata.predictions.peso', -1)]}
}


class FilesForm(FlaskForm):
    """Valida pesquisa de arquivos.

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """

    filtros = [
        ('0', 'Selecione uma opção'),
        ('1', 'Contêineres informados como vazios mas detectados ' +
         'como não vazios (ordem de peso detectado)')
    ]
    numero = StringField('Número', validators=[optional()])
    start = DateField('Start', validators=[optional()],
                      default=date.today() - timedelta(days=90))
    end = DateField('End', validators=[optional()], default=date.today())
    alerta = BooleanField('Alerta', validators=[optional()], default=False)
    pagina_atual = IntegerField('Pagina', default=1)
    filtro_auditoria = SelectField(u'Filtros de Auditoria', choices=filtros,
                                   default=0)


def recupera_user_filtros():
    """Usa variável global para guardar filtros personalizados entre posts."""
    global filtros
    filtro = {}
    if filtros.get(current_user.id):
        user_filtros = filtros[current_user.id]
    else:
        user_filtros = dict()
        filtros[current_user.id] = user_filtros
    if user_filtros:  # Adiciona filtro personalizado se houver
        for campo, valor in user_filtros.items():
            filtro[campo] = valor.lower()
    return filtro, user_filtros


def valida_form_files(form, filtro):
    """Lê formulário e adiciona campos ao filtro se necessário."""
    order = None
    pagina_atual = None
    if form.validate():  # configura filtro básico
        numero = form.numero.data
        start = form.start.data
        end = form.end.data
        alerta = form.alerta.data
        pagina_atual = form.pagina_atual.data
        filtro_escolhido = form.filtro_auditoria.data
        if filtro_escolhido:
            filtro_auditoria = FILTROS_AUDITORIA.get(filtro_escolhido)
            if filtro_auditoria:
                filtro.update(filtro_auditoria['filtro'])
                order = filtro_auditoria['order']
        if numero == 'None':
            numero = None
        if start and end:
            start = datetime.combine(start, datetime.min.time())
            end = datetime.combine(end, datetime.max.time())
            filtro['metadata.dataescaneamento'] = {'$lt': end, '$gt': start}
        if numero:
            filtro['metadata.numeroinformado'] = {'$regex': '^' + numero}
        if alerta:
            filtro['metadata.xml.alerta'] = True
        # print(filtro)
    return filtro, pagina_atual, order


@app.route('/files', methods=['GET', 'POST'])
@login_required
def files():
    """Recebe parâmetros, aplica no GridFS, retorna a lista de arquivos."""
    PAGE_ROWS = 50
    lista_arquivos = []
    campos = campos_chave()
    filtro = {}
    npaginas = 1
    pagina_atual = 1
    order = None
    form_files = FilesForm()
    filtro, user_filtros = recupera_user_filtros()
    if request.method == 'POST':
        form_files = FilesForm(**request.form)
        filtro, pagina_atual, order = valida_form_files(form_files, filtro)
    else:
        numero = request.args.get('numero')
        if numero:
            form_files = FilesForm(numero=numero)
            filtro['metadata.numeroinformado'] = {'$regex': '^' + numero}
    if filtro:
        filtro['metadata.contentType'] = 'image/jpeg'
        if order is None:
            order = [('metadata.dataescaneamento', 1)]
        if pagina_atual is None:
            pagina_atual = 1

        print(filtro)
        projection = {'_id': 1, 'filename': 1,
                      'metadata.numeroinformado': 1,
                      'metadata.predictions.bbox': 1,
                      'metadata.dataescaneamento': 1}
        skip = (pagina_atual - 1) * PAGE_ROWS
        count = db['fs.files'].find(filtro, {'_id'}
                                    ).limit(40 * PAGE_ROWS
                                            ).count(with_limit_and_skip=True)
        npaginas = count // PAGE_ROWS + 1
        # print('**Página:', pagina_atual, skip, type(skip))
        # print(count, skip)
        for grid_data in db['fs.files']\
            .find(filter=filtro, projection=projection)\
            .sort(order)\
                .limit(PAGE_ROWS).skip(skip):
            linha = {}
            linha['_id'] = grid_data['_id']
            linha['filename'] = grid_data['filename']
            linha['dataescaneamento'] = grid_data['metadata'].get(
                'dataescaneamento')
            linha['numero'] = grid_data['metadata'].get('numeroinformado')
            lista_arquivos.append(linha)
        # print(lista_arquivos)
        if len(lista_arquivos) < 50:
            npaginas = pagina_atual
    return render_template('search_files.html',
                           paginated_files=lista_arquivos,
                           oform=form_files,
                           campos=campos,
                           filtros=user_filtros,
                           npaginas=npaginas)


class StatsForm(FlaskForm):
    """Valida datas da tela de estatísticas."""

    start = DateField('Start', validators=[optional()],
                      default=date.today() - timedelta(days=90))
    end = DateField('End', validators=[optional()], default=date.today())


@app.route('/stats', methods=['GET', 'POST'])
@login_required
def stats():
    """Permite consulta as estatísticas do GridFS e integrações."""
    global stats_cache
    form = StatsForm(**request.form)
    if form.validate():
        start = datetime.combine(form.start.data, datetime.min.time())
        end = datetime.combine(form.end.data, datetime.max.time())
        stats_cache = stats_resumo_imagens(db, start, end)
    return render_template('stats.html',
                           stats=stats_cache,
                           oform=form)


@app.route('/pie')
def pie():
    """Renderiza gráfico no matplot e serializa via HTTP/HTML."""
    # TODO: matplotlib está falhando no multithread. Fazer dashboard
    # dash do AJNA e passar stats para lá.
    global stats_cache
    if stats_cache:
        stats = stats_cache['recinto']
        output = plot_pie(stats.values(), stats.keys())
    return Response(response=output.getvalue(), mimetype='image/png')


@app.route('/bars')
def bars():
    """Renderiza gráfico no matplot e serializa via HTTP/HTML."""
    global stats_cache
    if stats_cache:
        recinto = request.args.get('recinto')
        stats = stats_cache['recinto_mes'].get(recinto)
        if stats:
            output = plot_bar(stats.values(), stats.keys())
            return Response(response=output.getvalue(), mimetype='image/png')


@app.route('/padma_proxy/<image_id>')
def padma_proxy(image_id):
    """Teste. Envia uma imagem para padma teste e repassa retorno."""
    fs = GridFS(db)
    _id = ObjectId(image_id)
    if fs.exists(_id):
        grid_out = fs.get(_id)
        image = grid_out.read()
        # filename = grid_out.filename
        data = {}
        data['file'] = image
        headers = {}
        # headers['Content-Type'] = 'image/jpeg'
        r = requests.post(PADMA_URL + '/teste',
                          files=data, headers=headers)
        result = r.text
    return result


@nav.navigation()
def mynavbar():
    """Menu da aplicação."""
    items = [View('Home', 'index'),
             View('Importar Bson', 'upload_bson'),
             View('Pesquisar arquivos', 'files'),
             View('Estatísticas', 'stats'),
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
