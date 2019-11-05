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
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ajna_commons.flask.conf import SQL_URI

from ajna_commons.flask.log import logger
from virasana.integracao.mercante.mercantealchemy import Conhecimento, \
    ConteinerVazio, ControleResumo, Item, Manifesto, NCMItem, \
    t_conhecimentosEmbarque, t_ConteinerVazio, t_itensCarga, \
    t_manifestosCarga, t_NCMItemCarga


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
    Session = sessionmaker(bind=engine)
    session = Session()
    # Fazer INSERTS PRIMEIRO
    tipomovimento = 'I'
    controle = ControleResumo.get_(session, str(origem), tipomovimento)
    maxid = controle.maxid
    logger.info('%s - inicio em ID %s - tipo %s' % (origem, maxid, tipomovimento))
    s = select([origem]).where(
        and_(origem.c['id'] > maxid, origem.c['tipoMovimento'] == tipomovimento)
    ).order_by(origem.c['id']).limit(2000)
    cont = 0
    resulproxy = session.execute(s)
    keys = destino.__table__.columns.keys()
    keys.remove('ID')
    keys.remove('last_modified')
    newmaxid = maxid 
    for row in resulproxy:
        if row['id'] > newmaxid:
            newmaxid = row['id']
        dict_campos = {key: row[key]
                       for key in keys}
        objeto = destino(**dict_campos)
        session.add(objeto)
        cont += 1
        # chaves_valores = [getattr(destino, chave) == row[chave] for chave in chaves]
        # print(numeroCEmercante)
        # tipoMovimento = row[origem.c.tipoMovimento]
        # result_proxy = execute_movimento(session, destino, chaves_valores,
        #
        #                                 tipoMovimento, destino.__table__.columns.keys(), row)
    controle.maxid = newmaxid
    session.add(controle)
    session.commit()
    return cont


def mercante_resumo(engine):
    logger.info('Iniciando resumo da base Mercante...')
    migracoes = {t_conhecimentosEmbarque: Conhecimento}
    {
                 t_manifestosCarga: Manifesto,
                 t_itensCarga: Item,
                 t_NCMItemCarga: NCMItem,
                 t_ConteinerVazio: ConteinerVazio}

    chaves = {Conhecimento: ['numeroCEmercante'],
              Manifesto: ['numero'],
              Item: ['numeroCEmercante', 'numeroSequencialItemCarga'],
              NCMItem: ['numeroCEMercante', 'codigoConteiner',
                        'numeroSequencialItemCarga'],
              ConteinerVazio: ['manifesto', 'idConteinerVazio']
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
    engine = sqlalchemy.create_engine(SQL_URI)
    # engine = sqlalchemy.create_engine('sqlite:///teste.db')
    mercante_resumo(engine)
