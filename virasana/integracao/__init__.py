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
import io
from collections import OrderedDict
from datetime import datetime, timedelta
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib.pyplot as plt
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
        [{'$match': {'metadata.contentType': 'image/jpeg'}},
         {'$group':
          {'_id': '$metadata.recinto',

           'count': {'$sum': 1}}
          }])
    recintos = dict()
    for recinto in cursor:
        recintos[recinto['_id']] = recinto['count']
    ordered = OrderedDict(
        {key: recintos[key] for key in sorted(recintos)})
    stats['recinto'] = ordered
    return stats


def plot_pie(values, labels):
    """Gera gráfico de pizza."""
    fig1, ax1 = plt.subplots()
    ax1.pie(values, labels=labels, shadow=True)
    ax1.axis('equal')
    canvas = FigureCanvas(fig1)
    png = io.BytesIO()
    canvas.print_png(png)
    return png


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


def peso_container_documento(db, numeros: list):
    """Procedimento necessário para apurar o peso do contêiner.

    Procedimento necessário para apurar o peso do contêiner tendo em vista
    as informações importadas do CARGA.

    Args:
        db: conexão ao MongoDB GridFS
        numero: lista com os números de contêiner

    Returns:
        dict[numero] = peso

    """
    cursor = db['fs.files'].find(
        {'metadata.carga.atracacao.escala': {'$ne': None},
         'metadata.contentType': 'image/jpeg',
         'metadata.carga.container.container': {'$in': numeros}},
        {'_id': 0, 'metadata.carga.container.container': 1,
         'metadata.carga.container.pesobrutoitem': 1}
    )
    result = {}
    for linha in cursor:
        peso = 0.
        for item in linha['metadata']['container']:
            peso += float(item['pesobrutoitem'])
        result[linha['metadata']['container']['container']] = peso
    return result


def volume_container(db, numeros: list):
    """Procedimento necessário para apurar o volume do contêiner.

    Procedimento necessário para apurar o volume do contêiner tendo em vista
    as informações importadas do CARGA.

    Args:
        db: conexão ao MongoDB GridFS
        numero: lista com os números de contêiner

    Returns:
        dict[numero] = volume

    """
    cursor = db['fs.files'].find(
        {'metadata.carga.atracacao.escala': {'$ne': None},
         'metadata.contentType': 'image/jpeg',
         'metadata.carga.container.container': {'$in': numeros}},
        {'_id': 0, 'metadata.carga.container.container': 1,
         'metadata.carga.container.volumeitem': 1}
    )
    result = {}
    for linha in cursor:
        volume = 0.
        for item in linha['metadata']['container']:
            volume += float(item['volumeitem'])
        result[linha['metadata']['container']['container']] = volume
    return result


def peso_container_balanca(db, numero: list):
    """Procedimento necessário para apurar o peso pesado do contêiner.

    Procedimento necessário para apurar o peso do contêiner tendo em vista
    as informações importadas dos sistemas de pesagem dos Recintos Aduaneiros.

    Args:
        db: conexão ao MongoDB GridFS
        numero: lista com os números de

    Returns:
        dict[numero] = peso

    """
    pass


if __name__ == '__main__':
    print(stats_resumo_imagens)
