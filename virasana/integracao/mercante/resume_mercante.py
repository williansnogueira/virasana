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
from sqlalchemy.orm.exc import MultipleResultsFound

from virasana.integracao.mercante.mercantealchemy import Conhecimento, \
    ConteinerVazio, ControleResumo, Item, Manifesto, NCMItem, \
    t_conhecimentosEmbarque, t_ConteinerVazio, t_itensCarga, \
    t_manifestosCarga, t_NCMItemCarga


def get_pendentes(session, origem, tipomovimento, limit=10000):
    controle = ControleResumo.get_(session, str(origem), tipomovimento)
    maxid = controle.maxid
    logger.info('%s - inicio em ID %s - tipo %s' % (origem, maxid, tipomovimento))
    s = select([origem]).where(
        and_(origem.c['id'] > maxid, origem.c['tipoMovimento'] == tipomovimento)
    ).order_by(origem.c['id']).limit(limit)
    resultproxy = session.execute(s)
    return controle, resultproxy


def monta_campos(destino):
    keys = destino.__table__.columns.keys()
    keys.remove('ID')
    keys.remove('last_modified')
    return keys


def processa_resumo(engine, origem, destino, chaves):
    Session = sessionmaker(bind=engine)
    session = Session()
    # Fazer INSERTS PRIMEIRO
    movimentos = ['I', 'A', 'E']
    for tipomovimento in movimentos:
        controle, resultproxy = get_pendentes(session, origem, tipomovimento)
        campos_destino = monta_campos(destino)
        cont = 0
        for row in resultproxy:
            if row['id'] > controle.maxid:
                controle.maxid = row['id']
            dict_campos = {key: row[key]
                           for key in campos_destino}
            if tipomovimento == 'I':
                objeto = destino(**dict_campos)
                session.add(objeto)
                cont += 1
            else:  # A = Update / E = Delete
                chaves_valores = [getattr(destino, chave) == row[chave] for chave in chaves]
                # print(chaves_valores)
                try:
                    objeto = session.query(destino).filter(*chaves_valores).one_or_none()
                    if objeto:
                        cont += 1
                        if tipomovimento == 'E':
                            session.delete(objeto)
                            if hasattr(objeto, 'filhos'):
                                for filho in objeto.filhos:
                                    session.delete(filho)
                        else:
                            for k, v in dict_campos.items():
                                setattr(objeto, k, v)
                except MultipleResultsFound:
                    filtro = {chave: row[chave] for chave in chaves}
                    logger.error(
                        'Erro! Multiplos registros encontrados para %s com filtro %s'
                        'Registro %s não atualizado!' %
                        (destino.__tablename__, filtro, row['id'])
                    )
        session.add(controle)
        session.commit()
        logger.info('%s Resumos tipo %s processados' %
                    (cont, tipomovimento)
                    )


def mercante_resumo(engine):
    logger.info('Iniciando resumo da base Mercante...')
    migracoes = {t_conhecimentosEmbarque: Conhecimento,
                 t_manifestosCarga: Manifesto,
                 t_itensCarga: Item,
                 t_NCMItemCarga: NCMItem,
                 t_ConteinerVazio: ConteinerVazio}

    chaves = {Conhecimento: ['numeroCEmercante'],
              Manifesto: ['numero'],
              Item: ['numeroCEmercante', 'numeroSequencialItemCarga'],
              NCMItem: ['numeroCEMercante', 'codigoConteiner',
                        'numeroSequencialItemCarga', 'identificacaoNCM'],
              ConteinerVazio: ['manifesto', 'idConteinerVazio']
              }

    for origem, destino in migracoes.items():
        t0 = time.time()
        processa_resumo(engine, origem, destino, chaves[destino])
        t = time.time()
        logger.info('Resumos processados em %0.2f s' %
                    (t - t0)
                    )


if __name__ == '__main__':
    os.environ['DEBUG'] = '1'
    logger.setLevel(logging.DEBUG)
    engine = sqlalchemy.create_engine(SQL_URI)
    # engine = sqlalchemy.create_engine('sqlite:///teste.db')
    mercante_resumo(engine)
