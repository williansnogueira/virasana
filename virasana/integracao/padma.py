"""Módulo com funcões de gravar um cache no fs.files.

Funções para consultar PADMA e gravar predições no metadata
do GridFS.

"""
import os
import pickle
from json.decoder import JSONDecodeError

import pymongo
import requests

from ajna_commons.flask.conf import DATABASE, MONGODB_URI, PADMA_URL
from ajna_commons.flask.log import logger
from ajna_commons.flask.login import DBUser

USERNAME = 'virasana_service'
VIRASANA_PASS_FILE = os.path.join(os.path.dirname(__file__), USERNAME)
BBOX_MODELS = ['ssd']
CHAVES_PADMA = [
    'metadata.predictions.vazio',
    'metadata.predictions.peso',
    'metadata.predictions.volume',
    'metadata.predictions.ncms',
    'metadata.predictions.embalagens',
    'metadata.predictions.index',  # hash_linear
    'metadata.predictions.hash_semantic',
    'metadata.predictions.ameacas',
    'metadata.predictions.bbox'
]


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração."""
    for chave in CHAVES_PADMA:
        try:
            db['fs.files'].create_index(chave)
        except pymongo.errors.OperationFailure:
            pass


token = None


def get_service_password():
    """Retorna virasana_service password.

    Se não existir, cria password randômico e cria/atualiza usuário no DB.
    """
    password = None
    try:
        with open(VIRASANA_PASS_FILE, 'rb') as secret:
            try:
                password = pickle.load(secret)
            except pickle.PickleError:
                password = None
    except FileNotFoundError:
        password = None
    if password is None:
        password = str(os.urandom(24))
        db = pymongo.MongoClient(host=MONGODB_URI)[DATABASE]
        DBUser.dbsession = db
        DBUser.add(USERNAME, password)
        with open(VIRASANA_PASS_FILE, 'wb') as out:
            pickle.dump(password, out, pickle.HIGHEST_PROTOCOL)
    return USERNAME, password


def get_token(session, url):
    """Faz um get na url e tenta encontrar o csrf_token na resposta."""
    response = session.get(url)
    csrf_token = response.text
    begin = csrf_token.find('csrf_token"') + 10
    end = csrf_token.find('username"') - 10
    csrf_token = csrf_token[begin: end]
    begin = csrf_token.find('value="') + 7
    end = csrf_token.find('/>')
    csrf_token = csrf_token[begin: end]
    return csrf_token


def login(username, senha, session=None):
    """
    Autentica usuário no Servidor PADMA.

    Se não existir Usuário virasana, cria um com senha randômica
    """
    if session is None:
        session = requests.Session()
    url = PADMA_URL + '/login'
    csrf_token = get_token(session, url)
    # print('token*********', csrf_token)
    r = session.post(url, data=dict(
        username=username,
        senha=senha,
        csrf_token=csrf_token)
    )
    return r


def consulta_padma(image, model):
    """Monta request para o PADMA. Trata JSON resposta.

    Args:
        image: bytes image
        model: nome do modelo a consultar

    Returns:
        dict com as predições

    """
    global token
    data = {}
    data['image'] = image
    headers = {}
    result = {'predictions': [], 'success': False}
    s = requests.Session()
    username, password = get_service_password()
    if token is None:
        # print(username, password)
        r = login(username, password, s)
    try:
        r = s.post(PADMA_URL + '/predict?model=' + model,
                   files=data, headers=headers)
        if r.status_code == 200:
            result = r.json()
        # print(r.json())
    except JSONDecodeError as err:
        logger.error('Erro em consulta_padma(JSON inválido) %s HTTP Code:%s ' %
                     (err, r.status_code))
    return result


def interpreta_pred(prediction, model):
    """Resume predições se necessário."""
    if model == 'vazio':
        return prediction['1'] < 0.5
    if model == 'vaziosvm':
        return prediction['vazio']
    if model == 'peso':
        return prediction['peso']
    if model == 'index':
        return prediction['code']
    raise NotImplementedError('Modelo %s não implementado!' % model)


if __name__ == '__main__':
    db = pymongo.MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para predicitions')
    create_indexes(db)
