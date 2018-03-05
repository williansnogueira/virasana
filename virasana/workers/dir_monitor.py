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

VIRASANA_URL = "http://localhost:5001"
API_URL = VIRASANA_URL + "/api/uploadbson"
BSON_DIR = os.path.join('P:', 'SISTEMAS', 'roteiros', 'BSON')


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
    bson = open(filename, 'rb').read()
    data = {}
    data['file'] = (BytesIO(bson), 'test.bson')
    rv = requests.post(API_URL, data=data)
    print(rv)
    if rv is None:
        return False, None
    print(rv.text)
    # print(rv.json())
    return True, rv


def despacha_dir(dir=BSON_DIR, target=API_URL):
    """Envia por HTTP POST todos os arquivos do diretório.

    Args:
        dir: caminho completo do diretório a pesquisar
        target: URL do Servidor e API destino
    Returns:
        list of errors
    """
    lista = []
    for filename in os.listdir(dir):
        success, response = despacha(os.path.join(dir, filename), target)
        if not success:
            lista.append(response)


        
if __name__ == '__main__':
    print(despacha_dir())


