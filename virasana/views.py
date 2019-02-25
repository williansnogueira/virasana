"""Coleção de views da interface web do módulo Virasana.

Módulo Virasana é o Servidor de imagens e a interface para carga,
consulta e integração das imagens com outras bases.

"""
import json
import os
from base64 import b64encode
from datetime import date, datetime, timedelta
from sys import platform

import ajna_commons.flask.login as login_ajna
import requests
from ajna_commons.flask.conf import (BSON_REDIS, DATABASE, MONGODB_URI,
                                     PADMA_URL, SECRET, redisdb)
from ajna_commons.flask.log import logger
from ajna_commons.utils.images import mongo_image, recorta_imagem
from ajna_commons.utils.sanitiza import mongo_sanitizar
from bson import json_util
from bson.objectid import ObjectId
from flask import (Flask, Response, abort, flash, jsonify, redirect,
                   render_template, request, url_for)
from flask_bootstrap import Bootstrap
from flask_login import current_user, login_required
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

from virasana.integracao import (CHAVES_GRIDFS, carga, dict_to_html,
                                 dict_to_text, plot_bar_plotly,
                                 plot_pie_plotly, stats_resumo_imagens,
                                 summary)
from virasana.models.models import Ocorrencias, Tags
from virasana.utils.auditoria import FILTROS_AUDITORIA
from virasana.utils.image_search import ImageSearch
from virasana.workers.dir_monitor import BSON_DIR
from virasana.workers.tasks import raspa_dir, trata_bson

# TODO: Criar tabela para tags???


app = Flask(__name__, static_url_path='/static')
csrf = CSRFProtect(app)
Bootstrap(app)
nav = Nav()
nav.init_app(app)

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'static')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def configure_app(mongodb):
    """Configurações gerais e de Banco de Dados da Aplicação."""
    app.config['DEBUG'] = os.environ.get('DEBUG', 'None') == '1'
    if app.config['DEBUG'] is True:
        app.jinja_env.auto_reload = True
        app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.secret_key = SECRET
    app.config['SECRET_KEY'] = SECRET
    app.config['SESSION_TYPE'] = 'filesystem'
    login_ajna.configure(app)
    login_ajna.DBUser.dbsession = mongodb
    app.config['mongodb'] = mongodb
    try:
        img_search = ImageSearch(mongodb)
        app.config['img_search'] = img_search
    except (IOError, FileNotFoundError):
        pass
    return app


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
        return redirect(url_for('commons.login'))


def valid_file(file):
    """Valida arquivo passado e retorna validade e mensagem."""
    if not file or file.filename == '' or not allowed_file(file.filename):
        if not file:
            mensagem = 'Arquivo nao informado'
        elif not file.filename:
            mensagem = 'Nome do arquivo vazio'
        else:
            mensagem = 'Nome de arquivo não permitido: ' + \
                       file.filename
            # print(file)
        return False, mensagem
    return True, None


@app.route('/uploadbson', methods=['GET', 'POST'])
@csrf.exempt
# @login_required
def upload_bson():
    """Função simplificada para upload do arquivo de uma extração.

    Ver API/Upload BSON
    """
    taskid = ''
    if request.method == 'POST':
        # check if the post request has the file part
        file = request.files.get('file')
        validfile, mensagem = valid_file(file)
        if not validfile:
            flash(mensagem)
            return redirect(request.url)
        content = file.read()
        if platform == 'win32':
            with MongoClient(host=MONGODB_URI) as conn:
                db = conn[DATABASE]
                trata_bson(content, db)
        else:
            # print('Escrevendo no REDIS')
            d = {'bson': b64encode(content).decode('utf-8'),
                 'filename': file.filename}
            redisdb.rpush(BSON_REDIS + file.filename, json.dumps(d))
            result = raspa_dir.delay(file.filename)
            taskid = result.id
            # print('taskid', taskid)
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
    sync = request.form.get('sync', 'False')
    todir = request.form.get('todir', 'False')
    data = {'success': False,
            'mensagem': 'Task iniciada',
            'taskid': ''}
    try:
        # ensure a bson was properly uploaded to our endpoint
        file = request.files.get('file')
        validfile, mensagem = valid_file(file)
        if not validfile:
            data['mensagem'] = mensagem
            return jsonify(data)

        # else
        if todir == 'True':
            # Apenas salva em sistema de arquivo para carga posterior
            with open(os.path.join(BSON_DIR, file.filename), 'b') as out:
                file.write(out)
            return jsonify(data)

        # else
        content = file.read()
        if sync == 'True' or platform == 'win32':
            with MongoClient(host=MONGODB_URI) as conn:
                db = conn[DATABASE]
                trata_bson(content, db)
            data['success'] = True
        else:
            d = {'bson': b64encode(content).decode('utf-8'),
                 'filename': file.filename}
            redisdb.rpush(BSON_REDIS + file.filename, json.dumps(d))
            result = raspa_dir.delay(file.filename)
            data['taskid'] = result.id
            data['success'] = True
    except Exception as err:
        logger.error(err, exc_info=True)
        data['mensagem'] = 'Excecao ' + str(err)

    return jsonify(data)


@app.route('/api/task/<taskid>')
# @login_required
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
    db = app.config['mongodb']
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


@app.route('/summary/<_id>')
# @login_required
def summarytext(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe os metadados associados a ele.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    result = dict_to_text(summary(grid_data=grid_data)) + '\n' + \
             dict_to_text(carga.summary(grid_data=grid_data))
    return result


@app.route('/summaryhtml/<_id>')
@login_required
def summaryhtml(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe os metadados associados a ele.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    result = dict_to_html(summary(grid_data=grid_data))
    return result


@app.route('/summaryjson/<_id>')
# @login_required
def summaryjson(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe os metadados associados a ele.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    result = summary(grid_data=grid_data)
    result_carga = carga.summary(grid_data=grid_data)
    return jsonify({**result, **result_carga})


@app.route('/json/<_id>')
# @login_required
def json_get(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe os metadados associados a ele.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    grid_data = fs.get(ObjectId(_id))
    return json.dumps(grid_data.metadata,
                      sort_keys=True,
                      indent=4,
                      default=json_util.default)


class TagsForm(FlaskForm):
    tags = SelectField(u'Tags de usuário',
                       choices=Tags.list_tags(),
                       default=[0])


@app.route('/file/<_id>')
@app.route('/file')
@login_required
def file(_id=None):
    """Tela para exibição de um 'arquivo' do GridFS.

    Exibe o arquivo e os metadados associados a ele.
    """
    db = app.config['mongodb']
    fs = GridFS(db)
    form_tags = TagsForm()
    if request.args.get('filename'):
        filename = mongo_sanitizar(request.args.get('filename'))
        logger.warn('Filename %s ' % filename)
        grid_data = fs.find_one({'filename': filename})
    else:
        if not _id:
            _id = request.args.get('_id')
        grid_data = fs.get(ObjectId(_id))
    # print(grid_data)
    if grid_data:
        summary_ = dict_to_html(summary(grid_data=grid_data))
        summary_carga = dict_to_html(carga.summary(grid_data=grid_data))
        tags = Tags(db).list(_id)
        ocorrencias = Ocorrencias(db).list(_id)
    else:
        summary_ = summary_carga = 'Arquivo não encontrado.'
    return render_template('view_file.html',
                           myfile=grid_data,
                           summary=summary_,
                           summary_carga=summary_carga,
                           form_tags=form_tags, tags=tags,
                           ocorrencias=ocorrencias)


@login_required
@csrf.exempt
@app.route('/ocorrencia/add', methods=['POST', 'GET'])
def ocorrencia_add():
    """Função para inserção de ocorrência na imagem

    Faz update no fs.files, inserindo em um array o nome do usuário ativo
    e o texto da ocorrência passada.

    Args:
        _id: ObjectId do arquivo
        texto: String (texto)

    Returns:
        json['success']: True ou False

    """
    _id = request.values.get('_id')
    texto = request.values.get('texto')
    try:
        db = app.config['mongodb']
        ocorrencias = Ocorrencias(db)
        success = ocorrencias.add(_id=ObjectId(_id),
                                  usuario=current_user.id,
                                  texto=texto)
    except Exception as err:
        logger.error(err, exc_info=True)
        return jsonify({'erro': str(err)})
    return image_ocorrencia(_id, success)


@login_required
@csrf.exempt
@app.route('/ocorrencia/del', methods=['POST', 'GET'])
def ocorrencia_del():
    """Função para exclusão de ocorrência na imagem

    Faz update no fs.files, excluindo do array a id_ocorrencia

    Args:
        _id: ObjectId do arquivo
        id_ocorrencia: String (texto)

    Returns:
        image_ocorrencia, passando
        data['success']: True ou False

    """
    _id = request.values.get('_id')
    id_ocorrencia = request.values.get('id_ocorrencia')
    try:
        db = app.config['mongodb']
        ocorrencias = Ocorrencias(db)
        success = ocorrencias.delete(_id=ObjectId(_id),
                                     id_ocorrencia=id_ocorrencia)
    except Exception as err:
        logger.error(err, exc_info=True)
        return jsonify({'erro': str(err)})
    return image_ocorrencia(_id, success)


def image_ocorrencia(_id, success=True):
    """Função para listar ocorrências na imagem

    Args:
        _id: ObjectId do arquivo
        success: Falso se houve erro em operação anterior

    Returns:
        json['success']: True ou False

    """
    data = {'success': success}
    try:
        db = app.config['mongodb']
        ocorrencias = Ocorrencias(db)
        data['ocorrencias'] = ocorrencias.list(ObjectId(_id))
    except Exception as err:
        logger.error(err, exc_info=True)
        data['error'] = str(err)
        data['success'] = False
        # raise
    return jsonify(data)


@login_required
@csrf.exempt
@app.route('/tag/add', methods=['POST', 'GET'])
def tag_add():
    _id = request.values.get('_id')
    tag = request.values.get('tag')
    data = {'success': False}
    try:
        db = app.config['mongodb']
        tags = Tags(db)
        data['success'] = tags.add(_id=ObjectId(_id),
                                   usuario=current_user.id,
                                   tag=tag)
        data['tags'] = tags.list(ObjectId(_id))
    except Exception as err:
        logger.error(err, exc_info=True)
        data['error'] = str(err)
        # raise
    return jsonify(data)


@login_required
@csrf.exempt
@app.route('/tag/del', methods=['POST', 'GET'])
def tag_del():
    _id = request.values.get('_id')
    tag = request.values.get('tag')
    data = {'success': False}
    try:
        db = app.config['mongodb']
        tags = Tags(db)
        data['success'] = tags.delete(_id=ObjectId(_id),
                                      usuario=current_user.id,
                                      tag=tag)
        data['tags'] = tags.list(ObjectId(_id))
    except Exception as err:
        logger.error(err, exc_info=True)
        data['error'] = str(err)
        # raise
    return jsonify(data)


def image_tag(_id, tag):
    """Função para inserção de tag na imagem

    Faz update no fs.files, inserindo em um array com o nome do usuário ativo
    e a tag passada.

    Args:
        _id: ObjectId do arquivo
        tag: String (app usa lista de códigos com tupla (id, desc))

    Returns:
        json['success']: True ou False

    """
    _id = request.form.get('_id')
    tag = request.form.get('tag')
    data = {'success': False}
    try:
        db = app.config['mongodb']
        tags = Tags(db)
        data['success'] = tags.add(_id=ObjectId(_id),
                                   usuario=current_user.id,
                                   tag=tag)
        data['tags'] = tags.list(ObjectId(_id))
    except Exception as err:
        logger.error(err, exc_info=True)
        data['error'] = str(err)
        # raise

    return jsonify(data)


@app.route('/image')
@login_required
def image():
    """Executa uma consulta no banco.

    Monta um dicionário de consulta a partir dos argumentos do get.
    Se encontrar registro, chama image_id.
    """
    db = app.config['mongodb']
    filtro = {key: value for key, value in
              mongo_sanitizar(request.args.items())}
    linha = db['fs.files'].find_one(filtro, {'_id': 1})
    if linha:
        return image_id(linha['_id'])
    return ''


@app.route('/grid_data')
# @login_required
def grid_data():
    """Executa uma consulta no banco.

    Monta um dicionário de consulta a partir dos argumentos do get.
    Se encontrar registro, retorna registro inteiro via JSON (metadados),
    o arquivo (campo content) fica em fs.chunks e é recuperado pela view
    image_id.
    """
    # TODO: permitir consulta via POST de JSON
    db = app.config['mongodb']
    filtro = {mongo_sanitizar(key): mongo_sanitizar(value)
              for key, value in request.args.items()}
    logger.warning(filtro)
    linhas = db['fs.files'].find(filtro)
    result = [str(linha['_id']) for linha in linhas]
    return jsonify(result)


@app.route('/image/<_id>')
def image_id(_id):
    """Recupera a imagem do banco e serializa para stream HTTP.

    Estes métodos dispensam autenticação, pois é necessário ter um _id válido.
    """
    db = app.config['mongodb']
    image = mongo_image(db, _id)
    if image:
        return Response(response=image, mimetype='image/jpeg')
    return 'Sem Imagem'


def do_mini(_id, n):
    """Recupera, recorta a imagem do banco e serializa para stream HTTP."""
    db = app.config['mongodb']
    print('********n', n)
    fs = GridFS(db)
    _id = ObjectId(_id)
    if fs.exists(_id):
        print('********n', n)
        grid_data = fs.get(_id)
        if n is not None:
            n = int(n)
            preds = grid_data.metadata.get('predictions')
            if preds:
                bboxes = [pred.get('bbox') for pred in preds]
                if len(bboxes) >= n + 1 and bboxes[n]:
                    image = grid_data.read()
                    image = recorta_imagem(image, bboxes[n])
                    return Response(response=image, mimetype='image/jpeg')
    return 'Sem imagem'


@app.route('/mini1/<_id>')
def mini(_id, n=0):
    """Recorta a imagem do banco e serializa para stream HTTP."""
    return do_mini(_id, 0)


@app.route('/mini2/<_id>')
def mini2(_id):
    """Link para imagem do segundo contêiner, se houver."""
    return do_mini(_id, 1)


lista_ids = []
"""
Para testes de desempenho: o endpoint minitest retorna uma imagem aleatória
Se for passado o parâmetro 'mini', faz também recorte
Para habilitar, criar lista_ids descomentando linhas abaixo
lista_ids = [
    linha['_id'] for linha in
    db['fs.files'].find(
        {'metadata.contentType': 'image/jpeg'}, {'_id': 1}
    ).limit(1000)
]
"""


@app.route('/minitest')
def minitest():
    """Retorna uma imagem aleatória.

    Se for passado o parâmetro 'mini', faz também recorte
    Deixar desabilitado em produção.
    """
    if not lista_ids:
        return abort(404)
    import random
    _id = lista_ids[random.randint(0, 100)]
    n = request.args.get('mini')
    return do_mini(_id, n)


@app.route('/similar')
@login_required
def similar_():
    """Chama view de índice de imagens similares por GET.

    Recebe _id e offset(página atual).
    Para possibilitar rolagem de página.

    """
    _id = request.args.get('_id', '')
    offset = int(request.args.get('offset', 0))
    return similar(_id, offset)


@app.route('/similar/<_id>')
@login_required
def similar(_id, offset=0):
    """Retorna índice de imagens similares."""
    img_search = app.config['img_search']
    most_similar = img_search.get_chunk(_id, offset)
    return render_template('similar_files.html',
                           ids=most_similar,
                           _id=_id,
                           offset=offset,
                           chunk=img_search.chunk)


filtros = dict()


def campos_chave():
    """Retorna campos chave para montagem de filtro."""
    return CHAVES_GRIDFS + carga.CHAVES_CARGA


@app.route('/filtro_personalizado', methods=['GET', 'POST'])
@login_required
def filtro():
    """Configura filtro personalizado."""
    user_filtros = filtros[current_user.id]
    # print(request.form)
    # print(request.args)
    campo = request.args.get('campo')
    if campo:
        valor = request.args.get('valor')
        if valor:  # valor existe, adiciona
            user_filtros[campo] = mongo_sanitizar(valor)
        else:  # valor não existe, exclui chave
            user_filtros.pop(campo)
    result = [{'campo': k, 'valor': v} for k, v in user_filtros.items()]
    return jsonify(result)


class FilesForm(FlaskForm):
    """Valida pesquisa de arquivos.

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """

    filtros_auditoria_desc = [
        ('0', 'Selecione uma opção')
    ]
    for key, value in FILTROS_AUDITORIA.items():
        filtros_auditoria_desc.append((key, value['descricao']))
    numero = StringField(u'Número', validators=[optional()], default='')
    start = DateField('Start', validators=[optional()],
                      default=date.today() - timedelta(days=90))
    end = DateField('End', validators=[optional()], default=date.today())
    alerta = BooleanField('Alerta', validators=[optional()], default=False)
    pagina_atual = IntegerField('Pagina', default=1)
    filtro_auditoria = SelectField(u'Filtros de Auditoria',
                                   choices=filtros_auditoria_desc,
                                   default=0)
    filtro_tags = SelectField(u'Tags de usuário',
                              choices=sorted(
                                  Tags.list_tags(), key=lambda x: x[1]
                              ),
                              default=[0])


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
            filtro[campo] = valor
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
        tag_escolhida = form.filtro_tags.data
        print('****************************', tag_escolhida)
        if tag_escolhida and tag_escolhida != '0':
            filtro_tag = {'usuario': current_user.id, 'tag': tag_escolhida}
            filtro.update({'metadata.tags': {'$elemMatch': filtro_tag}})
        if numero == 'None':
            numero = None
        if start and end:
            start = datetime.combine(start, datetime.min.time())
            end = datetime.combine(end, datetime.max.time())
            filtro['metadata.dataescaneamento'] = {'$lt': end, '$gt': start}
        if numero:
            filtro['metadata.numeroinformado'] = \
                {'$regex': '^' + mongo_sanitizar(numero)}
        if alerta:
            filtro['metadata.xml.alerta'] = True
        # print(filtro)
    return filtro, pagina_atual, order


@app.route('/files', methods=['GET', 'POST'])
@login_required
def files():
    """Recebe parâmetros, aplica no GridFS, retorna a lista de arquivos."""
    db = app.config['mongodb']
    PAGE_ROWS = 50
    lista_arquivos = []
    campos = campos_chave()
    npaginas = 1
    pagina_atual = 1
    order = None
    form_files = FilesForm()
    filtro, user_filtros = recupera_user_filtros()
    if request.method == 'POST':
        print('****************************', request.form)
        form_files = FilesForm(**request.form)
        filtro, pagina_atual, order = valida_form_files(form_files, filtro)
    else:
        numero = request.args.get('numero')
        if numero:
            form_files = FilesForm(numero=numero)
            filtro['metadata.numeroinformado'] = \
                {'$regex': '^' + mongo_sanitizar(numero)}
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
        count = db['fs.files'].count_documents(filtro, limit=40 * PAGE_ROWS)
        print(count)
        # count = 100
        npaginas = (count - 1) // PAGE_ROWS + 1
        # print('**Página:', pagina_atual, skip, type(skip))
        # print(count, skip)
        for grid_data in db['fs.files'] \
                .find(filter=filtro, projection=projection) \
                .sort(order) \
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
    db = app.config['mongodb']
    global stats_cache
    form = StatsForm(**request.form)
    if form.validate():
        start = datetime.combine(form.start.data, datetime.min.time())
        end = datetime.combine(form.end.data, datetime.max.time())
        stats_cache = stats_resumo_imagens(db, start, end)
    return render_template('stats.html',
                           stats=stats_cache,
                           oform=form)


@app.route('/pie_plotly')
@login_required
def pie_plotly():
    """Renderiza HTML no pyplot e serializa via HTTP/HTML."""
    global stats_cache
    if stats_cache:
        stats = stats_cache['recinto']
        output = plot_pie_plotly(list(stats.values()), list(stats.keys()))
        return output
    return ''


@app.route('/bar_plotly')
@login_required
def bar_plotly():
    """Renderiza gráfico no plotly e serializa via HTTP/HTML."""
    global stats_cache
    print('stats_cache', stats_cache)
    if stats_cache:
        recinto = request.args.get('recinto')
        stats = stats_cache['recinto_mes'].get(recinto)
        if stats:
            output = plot_bar_plotly(list(stats.values()), list(stats.keys()))
            return output
    return ''


@app.route('/padma_proxy/<image_id>')
@login_required
def padma_proxy(image_id):
    """Teste. Envia uma imagem para padma teste e repassa retorno."""
    db = app.config['mongodb']
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

@app.route('/recarrega_imageindex')
@login_required
def recarrega_imageindex():
    """Recarrega image_index"""
    try:
        img_search = ImageSearch(app.config['mongodb'])
        app.config['img_search'] = img_search
    except (IOError, FileNotFoundError) as err:
        jsonify({'sucess': False, 'erro': str(err)})
    return jsonify({'sucess': True})


@nav.navigation()
def mynavbar():
    """Menu da aplicação."""
    items = [View('Home', 'index'),
             View('Importar Bson', 'upload_bson'),
             View('Pesquisar arquivos', 'files'),
             View('Estatísticas', 'stats'),
             ]
    if current_user.is_authenticated:
        items.append(View('Sair', 'commons.logout'))
    return Navbar(*items)


if __name__ == '__main__':
    # start the web server
    print('* Starting web service...')
    app.run(debug=app.config['DEBUG'])
