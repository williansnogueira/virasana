"""Funções para integrar ranking de risco de interposição.

Ranking de risco de interposição criado por Ivan Brasílico no âmbito do
curso de Data Mining da 8. Região Fiscal
"""
import csv
import os
from collections import defaultdict

from ajna_commons.flask.log import logger

FALTANTES = {'metadata.contentType': 'image/jpeg',
             'metadata.carga.conhecimento.cpfcnpjconsignatario': {'$exists': True},
             'metadata.ranking': {'$exists': False}}

CHAVES = [
    'metadata.ranking'
]
dict_empresas = {}
dict_rankings_cpfs = defaultdict(list)
try:
    ranking_file = os.path.join(os.path.dirname(__file__), 'empresas_risco.csv')
    with open(ranking_file, encoding='utf-8') as csv_in:
        reader = csv.reader(csv_in)
        next(reader)
        for row in reader:
            dict_rankings_cpfs[row[1]].append(float(row[3]))
        dict_empresas = {k: sum(v) / len(v)
                         for k, v in dict_rankings_cpfs.items()}
except FileNotFoundError as err:
    logger.error(err)


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração."""
    db['fs.files'].create_index('metadata.ranking')


def ranking_grava_fsfiles(db, batch_size=1000):
    """Busca por registros no GridFS sem info de Ranking

    Busca por registros no fs.files (GridFS - imagens) que não tenham metadata
    assignada de ranking de interposição.

    Args:
        db: connection to mongo with database setted

    Returns:
        Número de registros atualizados

    """
    filtro = FALTANTES
    projection = ['metadata.carga.conhecimento.cpfcnpjconsignatario']
    total = db['fs.files'].count_documents(filtro)
    lista_fs = list(
        db['fs.files'].find(filtro, projection=projection
                            ).limit(batch_size)[:batch_size]
    )
    lista_ranking = []
    for row in lista_fs:
        conhecimentos = row.get('metadata').get('carga').get('conhecimento')
        ranking_soma = 0.
        cont_conhecimento = 0.
        for conhecimento in conhecimentos:
            consignatario = conhecimento.get('cpfcnpjconsignatario')
            if consignatario is not None:
                # print(consignatario)
                cnpj_base = consignatario[:-6]
                # print(cnpj_base)
                ranking = dict_empresas.get(cnpj_base)
                if ranking is not None:
                    ranking_soma = ranking_soma + ranking
                    cont_conhecimento += 1.
        if ranking_soma > 0.:
            ranking_mean = ranking_soma / cont_conhecimento
            lista_ranking.append((row['_id'], ranking_mean))

    logger.info(
        'Resultado ranking_grava_fsfiles '
        'Pesquisados %s. '
        'Encontrados %s.'
        % (len(lista_fs), len(lista_ranking))
    )
    from pymongo import WriteConcern
    fsfiles_collection = db['fs.files'].with_options(
        write_concern=WriteConcern(w=0)
    )
    for out_row in lista_ranking:
        fsfiles_collection.update_one(
            {'_id': out_row[0]},
            {'$set': {'metadata.ranking': out_row[1]}}
        )
    return len(lista_ranking)


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    logger.info('Criando índice para Ranking')
    create_indexes(db)
    logger.info('Integrando rankings')
    logger.info('Atualizados %s registros de rankings' %
                ranking_grava_fsfiles(db, 100000))
