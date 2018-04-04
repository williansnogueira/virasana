import click
import time
import sys
from datetime import datetime

# from gridfs import GridFS
from pymongo import MongoClient

from virasana.integracao import create_indexes, carga

batch_size = 8000
today = datetime.today()

@click.command()
@click.option('--year', default=today.year, help='Ano')
@click.option('--month', default=today.month , help='mês')
@click.option('--batch_size', default=batch_size , help='Tamanho do lote')
@click.option('--interval', default=5 , help='Intervalo de dias')
def update(year, month, batch_size, interval):
    db = MongoClient()['test']
    create_indexes(db)
    carga.create_indexes(db)
    print('Começando a procurar por dados do CARGA a inserir')
    print(year, month, batch_size, interval)
    for day in range(1, 30, interval):
        data_inicio = datetime(year, month, day)
        print('Data início', data_inicio)
        tempo = time.time()
        carga.dados_carga_grava_fsfiles(db, batch_size, data_inicio, days=4)
        tempo = time.time() - tempo
        print(batch_size, 'dados Carga do fs.files percorridos em ',
              tempo, 'segundos.',
              tempo / batch_size, 'por registro')

if __name__=='__main__':
    update()