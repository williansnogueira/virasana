import sys
from sqlalchemy import create_engine

from ajna_commons.flask.conf import SQL_URI
from integracao.mercante.processa_xml_mercante import get_arquivos_novos, \
    xml_para_mercante
from integracao.mercante.resume_mercante import mercante_resumo


def do():
    lote = 100
    if len(sys.argv) > 1:
        lote = int(sys.argv[1])
        print('Lote de %s arquivos' % lote)
    print('Baixando arquivos novos...')
    sql = create_engine(SQL_URI)
    get_arquivos_novos(sql)
    xml_para_mercante(sql, lote)
    # mercante_resumo(sql)


if __name__ == '__main__':
    do()
