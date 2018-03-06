""" Monitora um diretório e envia arquivos BSON nele para o virasana.

USAGE
python dir_monitor.py

A cada intervalo de tempo configurado, lista os arquivos do diretório BSON_DIR
Se houverem arquivos, envia via POST para o endereco VIRASANA_URL

Pode ser importado e rodado em uma tarefa periódica (celery, cron, etc)

"""
import os
import time
from threading import Thread

import requests
from celery import states

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
    erro = response_json.get('success', False) and (
        rv.status_code == requests.codes.ok)
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
            bsonfile = os.path.join(dir, filename)
            success, response = despacha(bsonfile, target)
            if success:
                # TODO: save on database list of tasks
                response_json = response.json()
                taskid = response_json.get('taskid')
                sucessos.append(taskid)
                Thread(target=espera_resposta, args=(
                    VIRASANA_URL + '/api/task/' + taskid, bsonfile)).start()
            else:
                erros.append(response)
                logger.error(response.text)
        except Exception as err:
            exceptions.append(err)
            logger.error(err, exc_info=True)
    return erros, exceptions


def espera_resposta(api_url, bson_file, sleep_time=1, timeout=30):
    """Espera resposta da task.

    Espera resposta da task que efetivamente carregará o arquivo no
    Banco de Dados do Servidor.
    Recebendo uma resposta positiva, exclui arquivo enviado do disco.
    Recebendo uma resposta negativa, grava no logger.

    Args:
        api_url: endereço para acesso aos dados da task
        bson_file: caminho completo do arquivo original que foi enviado
        sleep_time: tempo entre requisições ao Servidor em segundos
        timeout: tempo total para aguardar resposta, em segundos
    """
    enter_time = time.time()
    rv = None
    try:
        while True:
            time.sleep(sleep_time)
            if time.time() - enter_time >= timeout:
                logger.error('Timeout ao esperar resultado de processamento ' +
                             'Funcao: espera_resposta' +
                             ' Arquivo: ' + bson_file)
                return False
            rv = requests.get(api_url)
            if rv and rv.status_code == 200:
                response_json = rv.json()
                state = response_json.get('state')
                if state and state in states.SUCCESS:
                    os.remove(bson_file)
                    logger.info('Arquivo ' + bson_file + ' removido.')
                    return True
                if state and state in states.FAILURE:
                    logger.error(rv.text)
                    return False
    except Exception as err:
        logger.error(err, exc_info=True)
        print(err)
    return False


if __name__ == '__main__':
    print(despacha_dir())
