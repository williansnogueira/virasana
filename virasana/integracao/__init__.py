"""
Funções padrão para exploração do GridFS.

Como repositório de informações do Banco de Dados, facilitando a documentação
e o desenvolvimento.

Assim, neste arquivo e nos demais deste módulo pode-se conferir como está
sendo estruturado o Banco de Dados final, que nada mais é do que a integração
de diversas fontes de dados, cada uma com seu módulo neste pacote. Pode-se
também conferir os campos chave para os quais estão sendo criados índices,
conferir as "chaves primária e estrangeira", as datas, categorias, etc.

Além disso, podem ser criadas e mantidas aqui funções que dêem estatíticas
sobre a base para informar os usuários.

"""
from datetime import datetime, timedelta
from virasana.integracao import carga
from virasana.integracao import xml

IMAGENS = {'metadata.contentType': 'image/jpeg'}

XML = {'metadata.contentType': 'text/xml'}

DATA = 'metadata.dataescaneamento'


def create_indexes(db):
    """Cria índices necessários no GridFS."""
    db['fs.files'].create_index('uploadDate')
    db['fs.files'].create_index('md5')
    db['fs.files'].create_index('filename')
    db['fs.files'].create_index('metadata.id')
    db['fs.files'].create_index('metadata.idcov')
    db['fs.files'].create_index('metadata.recintoid')
    db['fs.files'].create_index('metadata.recinto')
    db['fs.files'].create_index('metadata.imagem')
    db['fs.files'].create_index('metadata.numeroinformado')
    db['fs.files'].create_index('metadata.dataescaneamento')
    db['fs.files'].create_index('metadata.contentType')


def gridfs_count(db, filtro={}):
    """Aplica filtro, retorna contagem."""
    return db['fs.files'].find(filtro).count()


stats = {}
def stats_resumo_imagens(db):
    """Números gerais do Banco de Dados e suas integrações.

    Estatístics gerais sobre as imagens
    """
    global stats
    ultima_consulta = stats.get('data')
    now_atual = datetime.now()
    if ultima_consulta and \
        now_atual - ultima_consulta < timedelta(hours=1):
        return stats
    stats['data'] = now_atual
    total = gridfs_count(db, IMAGENS)
    stats['total'] = total
    stats['carga'] = total - gridfs_count(db, carga.FALTANTES)
    stats['xml'] = total - gridfs_count(db, xml.FALTANTES)
    linha = db['fs.files'].find(
        {'metadata.contentType': 'image/jpeg'}
    ).sort(DATA, 1).limit(1)
    linha = next(linha)
    for data_path in DATA.split('.'):
        linha = linha.get(data_path)
    stats['start'] = linha
    linha = db['fs.files'].find(
        {'metadata.contentType': 'image/jpeg'}
    ).sort(DATA, -1).limit(1)
    linha = next(linha)
    for data_path in DATA.split('.'):
        linha = linha.get(data_path)
    stats['end'] = linha
    # Qtde por Terminal
    cursor = db['fs.files'].aggregate(
        [{'$group':
          {'_id': '$metadata.recinto',
           
           'count': {'$sum': 1}}
          }])
    stats['recinto'] = {}
    for recinto in cursor:
        print(recinto)
        stats['recinto'][recinto['_id']] = recinto['count']
    return stats


def plot_bars(lista):
    """Gera gráfico de barras da lista de valores."""
    pass


def stats_por(db):
    """soon."""
    pass


def datas_bases():
    """Retorna nomes dos campos que possuem as datas de referência.

    Para cada integração, consulta se há data de referência e retorna

    """
    bases = {}
    bases['gridfs'] = DATA
    bases['xml'] = xml.DATA
    bases['carga'] = carga.DATA
    return bases


if __name__ == '__main__':
    print(stats_resumo_imagens)
