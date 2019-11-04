"""
Após a integração dos XMLs, resumir em tabelas de Estado

Os arquivos XML possuem movimentos: 'I'nclusao, 'A'tualizacao e 'E'xclusao
Este script consolida em tabelas.


Este arquivo pode ser chamado em um prompt de comando no Servidor ou
programado para rodar via crontab, conforme exempo em /periodic_updates.sh
"""
import logging
import os
import time
import sqlalchemy
from datetime import datetime
from sqlalchemy import func, select, and_

from ajna_commons.flask.log import logger
# from ajnaapi.config import Staging
from integracao.mercante.mercantealchemy import conhecimentos, conteineresVazios, \
    itens, manifestos, NCMItem, t_conhecimentosEmbarque, t_ConteinerVazio, \
    t_itensCarga, t_manifestosCarga, t_NCMItemCarga


def execute_movimento(conn, destino, chaves_valores,
                      tipoMovimento, keys, row):
    # print(tipoMovimento)
    # print(chaves_valores)
    if tipoMovimento == 'E':
        sql = destino.delete(
        ).where(and_(*chaves_valores))
        return conn.execute(sql)
    keys.remove('ID')
    keys.remove('last_modified')
    dict_campos = {key: row[key]
                   for key in keys}
    # Diferença entre banco MySQL e SQLite
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dict_campos['last_modified'] = timestamp
    if tipoMovimento == 'I':
        sql = destino.insert()
        # print(sql, dict_campos)
        try:
            return conn.execute(sql, **dict_campos)
        except sqlalchemy.exc.IntegrityError as err:
            print(err)
            # TODO: Ver como resolver o problema de processar duas vezes (criar flag??)
            pass
    # tipoMovimento == 'A':
    sql = destino.update(
    ).where(and_(*chaves_valores))
    return conn.execute(sql, **dict_campos)


def processa_resumo(engine, origem, destino, chaves):
    with engine.begin() as conn:
        s = select([func.Max(destino.c.create_date)])
        c = conn.execute(s).fetchone()
        start_date = 0
        if c and c[0] is not None:
            start_date = c[0]
        # print(c)
        print('Start date %s' % start_date)
        s = select([origem]
                   ).where(origem.c.create_date >= start_date)
        cont = 0
        for row in conn.execute(s):
            cont += 1
            chaves_valores = [destino.c[chave] == row[chave] for chave in chaves]
            # print(numeroCEmercante)
            tipoMovimento = row[origem.c.tipoMovimento]
            result_proxy = execute_movimento(conn, destino, chaves_valores,
                                             tipoMovimento, destino.c.keys(), row)
        return cont


def mercante_resumo(engine):
    logger.info('Iniciando resumo da base Mercante...')
    migracoes = {t_conhecimentosEmbarque: conhecimentos,
                 t_manifestosCarga: manifestos,
                 t_itensCarga: itens,
                 t_NCMItemCarga: NCMItem,
                 t_ConteinerVazio: conteineresVazios}

    chaves = {conhecimentos: ['numeroCEmercante'],
              manifestos: ['numero'],
              itens: ['numeroCEmercante', 'numeroSequencialItemCarga'],
              NCMItem: ['numeroCEMercante', 'codigoConteiner',
                        'numeroSequencialItemCarga'],
              conteineresVazios: ['manifesto', 'idConteinerVazio']
              }

    for origem, destino in migracoes.items():
        t0 = time.time()
        cont = processa_resumo(engine, origem, destino, chaves[destino])
        t = time.time()
        logger.info('%d registros processados em %0.2f s' %
                    (cont, t - t0)
                    )


if __name__ == '__main__':
    os.environ['DEBUG'] = '1'
    logger.setLevel(logging.DEBUG)
    # engine = sqlalchemy.create_engine('mysql+pymysql://ivan@localhost:3306/mercante')
    # engine = Staging.sql
    engine = sqlalchemy.create_engine('sqlite:///teste.db')
    mercante_resumo(engine)
