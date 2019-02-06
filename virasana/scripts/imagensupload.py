"""Script de linha de comando para processar diretório de arquivos BSON.

Script de linha de comando para fazer atualização 'manual'
processando diretório contendo arquivos BSON gerados pelo Avatar

Args:

    --dir: diretório a processar
    --sync: Fazer consulta de forma síncrona (True ou False)

"""
import os

import click

from virasana.workers.dir_monitor import despacha_dir

BSON_DIR = os.path.join('/home', 'ajna', 'Downloads', 'BSON')


@click.command()
@click.option('--dir', default=BSON_DIR,
              help='diretório a processar - padrão %s ' % BSON_DIR)
@click.option('--sync', is_flag=True,
              help='Fazer consulta de forma assíncrona')
def carrega(dir, sync):
    """Script de linha de comando para integração do arquivo XML."""
    print(despacha_dir(dir=dir, sync=sync))


if __name__ == '__main__':
    carrega()
