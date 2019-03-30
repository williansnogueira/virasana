"""Script de linha de comando para integração do Sistema CARGA.

Script de linha de comando para fazer atualização 'manual'
dos metadados do Sistema CARGA nas imagens.

Args:

    diainicio: dia de início da pesquisa
    diafim: dia final da pesquisa

"""
import time
from datetime import date, datetime, timedelta

import click

from virasana.integracao.carga2.manifesto import manifesto_grava_fsfiles
from virasana.db import mongodb as db

today = date.today()
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
    print('Começando a integração... Inicio %s Fim %s' % (start, end))
    ldata = start
    while ldata <= end:
        s0 = time.time()
        print('Integrando Manifestos dia %s  a %s...' % (ldata, ldata))
        manifesto_grava_fsfiles(db, ldata, ldata)
        s1 = time.time()
        print('Manifestos atualizados em %s segundos.' % (s1 - s0))
        ldata = ldata + timedelta(days=1)


if __name__ == '__main__':
    update()
