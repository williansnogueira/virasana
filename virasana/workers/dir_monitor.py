""" Monitora um diretório e envia arquivos BSON nele para o virasana.

USAGE
python dir_monitor.py

A cada intervalo de tempo configurado, lista os arquivos do diretório BSON_DIR
Se houverem arquivos, envia via POST para o endereco VIRASANA_URL

Pode ser importado e rodado em uma tarefa periódica (celery, cron, etc)

"""
import requests
import os
from io import BytesIO

from ajna_commons.flask.conf import VIRASANA_URL
from ajna_commons.flask.log import logger

# VIRASANA_URL = "http://localhost:5001"
API_URL = VIRASANA_URL + '/api/uploadbson'
BSON_DIR = os.path.join('P:', 'SISTEMAS', 'roteiros', 'BSON')
BSON_DIR = os.path.join(os.path.dirname(__file__),
                        '..', '..', '..', '..', 'files', 'BSON')


def despacha(filename, target=API_URL):
    """Envia por HTTP POST o arquivo especificado.

    Args:
        file: caminho completo do arquivo a enviar
        target: URL do Servidor e API destino
    Returns:
        (Erro, Resposta)
        (True, None) se tudo correr bem
        (False, response) se ocorrer erro
    """
    bson = open(filename, 'rb')
    files = {'file': bson}
    rv = requests.post(API_URL, files=files)
    if rv is None:
        return False, None
    response_json = rv.json()
    erro = response_json.get('success', False) and (rv.status_code == requests.codes.ok)
    return erro, rv


def despacha_dir(dir=BSON_DIR, target=API_URL):
    """Envia por HTTP POST todos os arquivos do diretório.

    Args:
        dir: caminho completo do diretório a pesquisar
        target: URL do Servidor e API destino
    Returns:
        list of errors
    """
    erros = []
    sucessos = []
    exceptions = []
    for filename in os.listdir(dir):
        try:
            success, response = despacha(os.path.join(dir, filename), target)
            if success:
                # TODO: save on database list of tasks
                response_json = response.json()
                sucessos.append(response_json.get('taskid'))
            else:
                erros.append(response)
                logger.error(response.text)
        except Exception as err:
            exceptions.append(err)
            logger.error(err, exc_info=True)
    return erros, exceptions


if __name__ == '__main__':
    print(despacha_dir())
