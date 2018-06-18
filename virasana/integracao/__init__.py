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
import plotly
import plotly.graph_objs as go

from collections import defaultdict, OrderedDict
from datetime import datetime
from pymongo import MongoClient

from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from virasana.integracao import carga
from virasana.integracao import xmli

IMAGENS = {'metadata.contentType': 'image/jpeg'}

XML = {'metadata.contentType': 'text/xml'}

DATA = 'metadata.dataescaneamento'

STATS_LIVE = 30  # Tempo em minutos para manter cache de stats
stats = {}

CHAVES_GRIDFS = [
    'uploadDate',
    'md5',
    'filename',
    'metadata.id',
    'metadata.idcov',
    'metadata.recintoid',
    'metadata.recinto',
    'metadata.imagem',
    'metadata.numeroinformado',
    'metadata.dataescaneamento',
    'metadata.contentType'
]


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
    return db['fs.files'].find(filtro).count(with_limit_and_skip=True)


def tag(word: str, tags: list):
    """Coloca tags em torno de word."""
    open_tags = ['<' + tag + '>' for tag in tags]
    close_tags = ['</' + tag + '>' for tag in reversed(tags)]
    print('***************', word)
    return ''.join(open_tags) + word + ''.join(close_tags)


def dict_to_html(adict: dict):
    """Retorna HTML."""
    lista = []
    LABEL = ['span', 'b']
    TEXT = ['span', 'br']
    for key, value in adict.items():
        lista.append(tag(key, LABEL))
        if isinstance(value, str):
            val = value
        elif isinstance(value, list):
            val = '<br>'.join(value)
        else:
            val = 'Linha tipo ' + type(value) + ' não suportada.'
        lista.append(tag(val, TEXT))
    return '\n'.join(lista)


def dict_to_text(adict: dict):
    r"""Retorna key\n [value\n]."""
    lista = []
    for key, value in adict.items():
        lista.append(key)
        if isinstance(value, str):
            lista.append(value)
        elif isinstance(value, list):
            lista.append('\n'.join(value))
        else:
            lista.append('Linha tipo ' + type(value) + ' não suportada.')
    return '\n'.join(lista)


def summary(grid_data=None, registro=None):
    """Selecionar campos mais importantes para exibição.

    Args:
        grid_data: Registro GridData do Gridfs.get
        registro: registro (dict) lido de cursor do MongoDB

    Returns:
        dict com descrição e valor de campos. (str: str)

    """
    result = {}
    if grid_data:
        meta = grid_data.metadata
        upload = grid_data.uploadDate.strftime('%Y-%m-%d %H:%M')
    else:
        meta = registro.get('metadata')
        upload = meta.get('uploadDate')
    if not meta:
        raise TypeError('Não foi passado registro válido' +
                        'para a função integracao.__init__.summary')
    result['Número contêiner informado pelo recinto'] = meta.get(
        'numeroinformado')
    result['Data de escaneamento'] = meta.get(
        'dataescaneamento').strftime('%Y-%m-%d %H:%M')
    result['Data de Carregamento da imagem no sistema'] = upload
    result['Nome Recinto'] = meta.get('recinto')
    return result


def stats_resumo_imagens(db, datainicio=None, datafim=None):
    """Números gerais do Banco de Dados e suas integrações.

    Estatísticas gerais sobre as imagens
    """
    # TODO: Extremamente lento, guardar estatísticas em tabelas à parte
    # e rodar de forma batch ou criar counters
    # import cProfile, pstats, io
    # pr = cProfile.Profile()
    # pr.enable()
    # datainicio = datetime(2018,1,1)
    # datafim = datetime(2018,2,1)
    stats = {}
    filtro = IMAGENS
    if datainicio and datafim:
        print(datainicio, datafim)
        filtro['metadata.dataescaneamento'] = {
            '$gt': datainicio, '$lt': datafim}
    now_atual = datetime.now()
    stats['Data do levantamento'] = now_atual
    total = gridfs_count(db, filtro)
    stats['Total de imagens'] = total
    stats['Imagens com info do Carga'] = total - \
        gridfs_count(db, dict(filtro, **carga.FALTANTES))
    stats['Imagens com info do XML'] = total - \
        gridfs_count(db, dict(filtro, **xmli.FALTANTES))
    # DATAS
    datas = {'imagem': DATA,
             'XML': xmli.DATA,
             'Carga': carga.DATA}
    for base, data in datas.items():
        # print(data)
        filtro_data = dict(filtro)
        if data != DATA:
            filtro_data[data] = {'$ne': None}
        linha = db['fs.files'].find(filtro_data).sort(data, 1).limit(1)
        try:
            linha = next(linha)
            for data_path in data.split('.'):
                if linha:
                    linha = linha.get(data_path)
            if isinstance(linha, datetime):
                linha = linha.strftime('%d/%m/%Y %H:%M:%S %z')
            stats['Menor ' + data_path + ' ' + base] = linha
            linha = db['fs.files'].find(filtro_data).sort(data, -1).limit(1)
            linha = next(linha)
            for data_path in data.split('.'):
                if linha:
                    linha = linha.get(data_path)
            if isinstance(linha, datetime):
                linha = linha.strftime('%d/%m/%Y %H:%M:%S %z')
            stats['Maior ' + data_path + ' ' + base] = linha
        except StopIteration:  # Não há registro nas datas filtradas
            pass
    # Qtde por Terminal
    cursor = db['fs.files'].aggregate(
        [{'$match': filtro},
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
    cursor = db['fs.files'].aggregate(
        [{'$match': {'metadata.contentType': 'image/jpeg'}},
         {'$project':
            {'month': {'$month': '$metadata.dataescaneamento'},
             'year': {'$year': '$metadata.dataescaneamento'},
             'recinto': '$metadata.recinto'
             }
          },
         {'$group':
            {'_id':
                {'recinto': '$recinto', 'month': '$month', 'year': '$year'},
             'count': {'$sum': 1}
             }
          }
         ])
    recinto_mes = defaultdict(dict)
    for linha in cursor:
        recinto = linha['_id']['recinto']
        ano_mes = '%04d%02d' % (linha['_id']['year'],
                                linha['_id']['month'])
        recinto_mes[recinto][ano_mes] = linha['count']
    for recinto, value in recinto_mes.items():
        ordered = OrderedDict(
            {key: value[key] for key in sorted(value)})
        recinto_mes[recinto] = ordered
    stats['recinto_mes'] = recinto_mes
    # pr.disable()
    # s = io.StringIO()
    # sortby = 'cumulative'
    # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    # ps.print_stats()
    # print(s.getvalue())
    return stats


def plot_pie_plotly(values, labels):
    """Gera gráfico de terminais."""
    # labels = ['1', '2', '3']
    # values =  [10, 20, 30]
    print(values, labels)
    plot = plotly.offline.plot({
        'data': [go.Pie(labels=labels, values=values)],
        'layout': go.Layout(title='Imagens por Recinto')
    },
        show_link=False,
        output_type='div')
    return plot


def plot_bar_plotly(values, labels):
    """Gera gráfico de barras."""
    # x = list(range(len(labels)))
    print(labels, values)
    plot = plotly.offline.plot({
        'data': [go.Bar(x=labels, y=values)],
        'layout': go.Layout(title='',
                            xaxis=go.XAxis(type='category'))
    },
        show_link=False,
        output_type='div',
        image_width=400)
    return plot


def stats_por(db):
    """soon."""
    pass


def datas_bases():
    """Retorna nomes dos campos que possuem as datas de referência.

    Para cada integração, consulta se há data de referência e retorna

    """
    bases = {}
    bases['gridfs'] = DATA
    bases['xml'] = xmli.DATA
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
    # Contêiner pode ter uso parcial, portanto é necessário somar
    for linha in cursor:
        peso = 0.
        for item in linha['metadata']['carga']['container']:
            peso += float(item['pesobrutoitem'].replace(',', '.'))
        result[linha['metadata']['carga']['container'][0]['container']] = peso
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
    # Contêiner pode ter uso parcial, portanto é necessário somar
    result = {}
    for linha in cursor:
        volume = 0.
        for item in linha['metadata']['carga']['container']:
            volume += float(item['volumeitem'].replace(',', '.'))
        result[linha['metadata']['carga']
               ['container'][0]['container']] = volume
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
    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para metadata')
    create_indexes(db)
    # print(stats_resumo_imagens(db))
