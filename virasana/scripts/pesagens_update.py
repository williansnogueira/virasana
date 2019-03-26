"""Script de linha de comando para integração das pesagens ADE02/2003

Script de linha de comando para fazer atualização 'manual'
dos metadados das pesagens nas imagens

Args:

    diainicio: dia de início da pesquisa
    diafim: dia final da pesquisa

"""
import time
from datetime import datetime, timedelta

import click

from virasana.db import mongodb as db
from virasana.integracao import info_ade02

today = datetime.today()
str_today = datetime.strftime(today, '%d/%m/%Y')
yesterday = today - timedelta(days=1)
str_yesterday = datetime.strftime(yesterday, '%d/%m/%Y')


@click.command()
@click.option('--inicio', default=str_yesterday,
              help='Dia de início (dia/mês/ano) - padrão ontem')
@click.option('--fim', default=str_yesterday,
              help='Dia de fim (dia/mês/ano) - padrão ontem')
def update(inicio, fim):
    """Script de linha de comando para integração do arquivo XML."""
    start = datetime.strptime(inicio, '%d/%m/%Y')
    end = datetime.strptime(fim, '%d/%m/%Y')
    print('Criando índices')
    info_ade02.create_indexes(db)
    print('Começando a integração... Inicio %s Fim %s' % (inicio, fim))
    tempo = time.time()
    print('Adquirindo pesagens do dia')
    print(info_ade02.adquire_pesagens(db, start, end))
    print('Integrando pesagens do dia')
    qtde = info_ade02.pesagens_grava_fsfiles(db, start, end)
    tempo = time.time() - tempo
    tempo_registro = 0 if (qtde == 0) else (tempo / qtde)
    print(qtde, 'dados de pesagem inseridos em fs.files em ',
          tempo, 'segundos.',
          tempo_registro, 'por registro')


if __name__ == '__main__':
    update()
