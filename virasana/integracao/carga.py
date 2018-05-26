"""Funçãoes para importar os dados da base CARGA do Bhadrasana."""

import csv
import io
import os
import pprint
import typing
from collections import Counter, OrderedDict
from datetime import datetime, timedelta
from zipfile import ZipFile

import pymongo

from ajna_commons.conf import ENCODE
from ajna_commons.flask.log import logger

FALTANTES = {'metadata.carga.atracacao.escala': None,
             'metadata.contentType': 'image/jpeg'}

ENCONTRADOS = {'metadata.carga.atracacao.escala': {'$ne': None},
               'metadata.contentType': 'image/jpeg'}

NUMERO = 'metadata.carga.container.container'

DATA = 'metadata.carga.atracacao.dataatracacaoiso'

# db['fs.files'].find({'metadata.contentType': 'image/jpeg'}).sort(
# metadata.carga.atracacao.dataatracacao, -1).limit(10)


CHAVES_CARGA = [
    'metadata.carga.vazio',
    'metadata.carga.atracacao.escala',
    'metadata.carga.manifesto.manifesto',
    'metadata.carga.conhecimento.conhecimento',
    'metadata.carga.conhecimento.cpfcnpjconsignatario',
    'metadata.carga.container.container',
    'metadata.carga.ncm.ncm',
    'metadata.carga.atracacao.dataatracacao',
]


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração.

    São criados índices para desempenho nas consultas.
    Alguns índices únicos também são criados, estes para evitar importação
    duplicada do mesmo registro.
    """
    db['CARGA.ContainerVazio'].create_index('container')
    db['CARGA.ContainerVazio'].create_index('manifesto')
    db['CARGA.ContainerVazio'].create_index(
        [('manifesto', pymongo.ASCENDING),
         ('container', pymongo.ASCENDING)],
        unique=True)
    """
    cursor = db['CARGA.EscalaManifesto'].aggregate(
        [{'$group':
          {'_id': ['$manifesto', '$escala'],
           'dups': {'$push': '$_id'},
           'count': {'$sum': 1}}},
            {'$match': {'count': {'$gt': 1}}}]
    )
    for ind, cursor in enumerate(cursor):
        ids = cursor['dups']
        for _id in ids[1:]:
            db['CARGA.EscalaManifesto'].remove(_id)
    print('TOTAL de registros duplicados', ind)
    """
    db['CARGA.EscalaManifesto'].create_index('manifesto')
    db['CARGA.EscalaManifesto'].create_index('escala')
    db['CARGA.EscalaManifesto'].create_index(
        [('manifesto', pymongo.ASCENDING),
         ('escala', pymongo.ASCENDING)],
        unique=True)
    db['CARGA.Escala'].create_index('escala', unique=True)
    db['CARGA.Container'].create_index('container')
    db['CARGA.Container'].create_index('conhecimento')
    db['CARGA.Conhecimento'].create_index('conhecimento', unique=True)
    db['CARGA.ManifestoConhecimento'].create_index('conhecimento')
    db['CARGA.ManifestoConhecimento'].create_index('manifesto')
    db['CARGA.AtracDesatracEscala'].create_index('escala')
    db['CARGA.AtracDesatracEscala'].create_index('manifesto')
    db['CARGA.Manifesto'].create_index('manifesto', unique=True)
    db['CARGA.NCM'].create_index('conhecimento')
    db['CARGA.NCM'].create_index(
        [('conhecimento', pymongo.ASCENDING),
         ('item', pymongo.ASCENDING)],
        unique=True)
    db['CARGA.Container'].create_index(
        [('conhecimento', pymongo.ASCENDING),
         ('container', pymongo.ASCENDING),
         ('item', pymongo.ASCENDING)],
        unique=True)
    # Cria campos utilizados para pesquisa de imagens
    for campo in CHAVES_CARGA:
        db['fs.files'].create_index(campo, sparse=True)
    # Cria campo data de atracacao no padrão ISODate
    cursor = db['CARGA.AtracDesatracEscala'].find({'dataatracacaoiso': None})
    for linha in cursor:
        dataatracacao = linha['dataatracacao']
        horaatracacao = linha['horaatracacao']
        dataatracacaoiso = datetime.strptime(dataatracacao + horaatracacao,
                                             '%d/%m/%Y%H:%M:%S')
        # print(linha['_id'], dataatracacao, dataatracacaoiso)
        db['CARGA.AtracDesatracEscala'].update(
            {'_id': linha['_id']}, {
                '$set': {'dataatracacaoiso': dataatracacaoiso}}
        )
    db['fs.files'].create_index('metadata.carga.atracacao.dataatracacaoiso')


def mongo_find_in(db, collection: str, field: str, in_set,
                  set_field: str=None,
                  filtros: dict=None) -> typing.Tuple[list, set]:
    """Realiza um find $in in_set no db.collection.

    Args:
        db: conexão ao MongoDB com banco de dados selecionado "setted"

        collection: nome da coleção mongo para aplicar a "query"

        field: campo para aplicar a "filter by"

        in_set: lista ou conjunto de valores a passar para o operador "$in"

        set_field: campo para obter valores únicos (opcional)

        filtros: filtros adicionais a aplicar

    Returns:
        Dicionário de resultados formatado key:value(Somente campos não nulos)
        Conjuntos de set_field

    """
    result = []
    field_set = set()
    filtro = {field: {'$in': list(in_set)}}
    if filtros:
        filtro.update(filtros)
    # print(filtro)
    cursor = db[collection].find(filtro)
    for linha in cursor:
        result.append(
            {str(key): field for key, field in linha.items()
             if field is not None})
        if set_field:
            field_set.add(linha[set_field])
    # print(result)
    # print(field_set)
    return result, field_set


def busca_atracacao_data(atracacoes: list, scan_datetime: datetime,
                         days) -> int:
    """Pega da lista de atracações a atracação com a data mais próxima.

    Args:
        atracacoes: lista de dict contendo os registros das atracacoes

        scan_datetime: data de partida

        data: data buscada

        days: "threshold"  máxima diferença entre as datas

    Returns:
        Índice da atracação, None se atracação não existe ou não está no
        intervalo threshold

    """
    index = None
    threshold = timedelta(days=days)
    for ind, atracacao in enumerate(atracacoes):
        data = atracacao['dataatracacao']
        hora = atracacao['horaatracacao']
        datahora = datetime.strptime(data + hora, '%d/%m/%Y%H:%M:%S')
        datetimedelta = abs(datahora - scan_datetime)
        # print(datetimedelta, threshold)
        if datetimedelta < threshold:
            threshold = datetimedelta
            index = ind
    return index


def get_escalas(db, conhecimentos_set: set, scan_datetime: datetime,
                days: int, exportacao=False)-> typing.Tuple[list, list, int]:
    """Dada uma lista de conhecimentos, retorna a lista de escalas.

    Retorna a lista de escalas vinculadas, com datadeatracao entre
    scan_datetime e scan_datetime + days

    Args:
        conhecimentos: lista de dict contendo os registros das atracacoes

        scan_datetime: data de partida

        days: "threshold"  máxima diferença entre as datas

    Returns:
        (manifestos, escalas)

    """
    if exportacao:
        days = days * -2
        filtros = {'tipomanifesto': 'lce'}
    else:
        filtros = {'tipomanifesto': {'$in': ['lci', 'bce']}}
    manifestos, manifestos_set = mongo_find_in(
        db, 'CARGA.ManifestoConhecimento', 'conhecimento',
        conhecimentos_set, 'manifesto', filtros)
    escalas, escalas_set = mongo_find_in(
        db, 'CARGA.EscalaManifesto', 'manifesto', manifestos_set, 'escala')
    atracacoes, _ = mongo_find_in(
        db, 'CARGA.AtracDesatracEscala', 'escala', escalas_set)
    index_atracacao = busca_atracacao_data(atracacoes, scan_datetime, days)
    return manifestos, escalas, atracacoes, index_atracacao


def busca_info_container(db, numero: str,
                         data_escaneamento: datetime, days=-5) -> dict:
    """Busca heurística na base CARGA MongoDB de dados sobre o Contêiner.

    A busca é baseada na data de escaneamento. O parâmetro dias é um
    "threshold" (diferença aceita entre a data de atracação e escaneamento),
    por padrão, é de -5 dias na importação e +10 dias na importação.

    Dentro destes 5 dias, será considerado o CE/Manifesto/Escala com menor
    diferença de data como o pertencente a este contêiner.
    Note-se que o resultado não é garantido, podendo trazer um CE incorreto.

    As informações são imperfeitas e não há como garantir trazer o CE correto,
    mas espera-se um acerto próximo de 100%, já que a frequência de cada
    contêiner em cada porto tem um intervalo típico de semanas e até meses,
    sendo extremamente incomum um contêiner ter duas "viagens" no mesmo
    porto em menos de 5 dias +/-.

    Args:
        numero: número completo do contêiner

        data_escaneamento: data e hora do escaneamento, conforme
        arquivo XML original do escâner

        days: número de dias a aceitar de diferença

    Returns:
        json_dict: Dicionário com campos e valores de informações da base
        CARGA VAZIO se não encontrar nada dentro do threshold
        (Caso não encontre atracacao para o Contêiner no prazo, o dado ?ainda?
        não existe ou não foi importado ou há um erro)!

    """
    json_dict = {}
    json_dict_vazio = {}
    numero = numero.casefold()
    # Primeiro busca por contêiner vazio (dez vezes mais rápido)
    containeres_vazios, manifestos_vazios_set = mongo_find_in(
        db, 'CARGA.ContainerVazio', 'container', set([numero]), 'manifesto')
    # print(containeres_vazios)
    escalas_vazios, escalas_vazios_set = mongo_find_in(
        db, 'CARGA.EscalaManifesto', 'manifesto', manifestos_vazios_set,
        'escala')
    # print('escalas', escalas_vazios)
    atracacoes_vazios, _ = mongo_find_in(
        db, 'CARGA.AtracDesatracEscala', 'escala', escalas_vazios_set)
    # print('atracacoes', atracacoes_vazios)
    index_atracacao_vazio = busca_atracacao_data(
        atracacoes_vazios, data_escaneamento, days)
    # print('INDEX', index_atracacao)
    if index_atracacao_vazio is not None:
        atracacao = atracacoes_vazios[index_atracacao_vazio]
        json_dict_vazio['atracacao'] = atracacao
        json_dict_vazio['vazio'] = True
        manifesto = [linha['manifesto'] for linha in escalas_vazios
                     if linha['escala'] == atracacao['escala']]

        json_dict_vazio['manifesto'], _ = mongo_find_in(
            db, 'CARGA.Manifesto', 'manifesto', manifesto)
        json_dict_vazio['container'] = [linha for linha in containeres_vazios
                                        if linha['manifesto'] == manifesto[0]]
    # Verificar se tem CE. Priorizar CE se tiver
    containeres, conhecimentos_set = mongo_find_in(
        db, 'CARGA.Container', 'container', set([numero]), 'conhecimento')
    conhecimentos, _ = mongo_find_in(
        db, 'CARGA.Conhecimento', 'conhecimento', conhecimentos_set)
    # Busca CE Exportação
    manifestos, escalas, atracacoes, index_atracacao = get_escalas(db, conhecimentos,
                                                          data_escaneamento,
                                                          days,
                                                          exportacao=True)
    if index_atracacao is None:
        manifestos, escalas, atracacoes, index_atracacao = get_escalas(db, conhecimentos,
                                                              data_escaneamento,
                                                              days)
    if index_atracacao is not None:
        # Agora sabemos o número do(s) CE(s) corretos do contêiner
        # Basta montar uma estrutura de dados com as informações
        atracacao = atracacoes[index_atracacao]
        json_dict['vazio'] = False
        json_dict['atracacao'] = atracacao
        manifesto = [linha['manifesto'] for linha in escalas
                     if linha['escala'] == atracacao['escala']]
        json_dict['manifesto'], _ = mongo_find_in(
            db, 'CARGA.Manifesto', 'manifesto', manifesto)
        conhecimentos = [linha['conhecimento'] for linha in manifestos
                         if linha['manifesto'] == manifesto[0]]

        # Separar APENAS os Conhecimentos BL ou MBL
        filtro = {'conhecimento': {'$in': list(conhecimentos)},
                  'tipo': {'$in': ['bl', 'mbl']}}
        cursor = db['CARGA.Conhecimento'].find(filtro, {'conhecimento': 1})
        conhecimentos = [linha['conhecimento'] for linha in cursor]

        json_dict['conhecimento'], _ = mongo_find_in(
            db, 'CARGA.Conhecimento', 'conhecimento', conhecimentos)
        json_dict['ncm'], _ = mongo_find_in(
            db, 'CARGA.NCM', 'conhecimento', conhecimentos)
        json_dict['container'] = [linha for linha in containeres
                                  if linha['conhecimento'] in conhecimentos]
        return json_dict
    return json_dict_vazio


def dados_carga_grava_fsfiles(db, batch_size=1000,
                              data_inicio=datetime(1900, 1, 1),
                              days=4,
                              update=True, force_update=False):
    """Busca por registros no GridFS sem info do CARGA.

    Busca por registros no fs.files (GridFS - imagens) que não tenham metadata
    importada do sistema CARGA. Itera estes registros, consultando a
    busca_info_container para ver se retorna informações do CARGA. Encontrando
    estas informações, grava no campo metadata.carga do fs.files

    Args:
        db: conexão com o banco de dados selecionado.

        batch_size: número de registros a consultar/atualizar por chamada

        data_inicio: filtra por data de escaneamento maior que a informada

        days: número de dias a aceitar de diferença

        update: Caso seja setado como False, apenas faz consulta, sem
        atualizar metadata da collection fs.files

        force_update: Marcar "NA" - not available se não encontrar dados do
        CARGA. Usar com consciência/cuidado.

    Returns:
        Número de registros encontrados

    """
    filtro = FALTANTES
    filtro['metadata.dataescaneamento'] = \
        {'$gt': data_inicio}
    #         '$lt': data_inicio + timedelta(days=days * 2)}
    # print(filtro)
    file_cursor = db['fs.files'].find(filtro).sort(
        'metadata.dataescaneamento', 1)
    acum = 0
    total = min(file_cursor.count(), batch_size)
    start = datetime.utcnow()
    if total == 0:
        logger.info('dados_carga_grava_fsfiles sem arquivos para processar')
        return 0
    end = start - timedelta(days=10000)
    for linha in file_cursor.limit(batch_size):
        container = linha.get('metadata').get('numeroinformado')
        if container:  # Lembrar que está tudo minusculo no BD!
            container = container.lower()
        data = linha.get('metadata').get('dataescaneamento')
        # print(container, data)
        if data and container:
            if data < start:
                start = data
            if data > end:
                end = data
            dados_carga = busca_info_container(db, container, data, days)
            if dados_carga != {}:
                if update:
                    # print(dados_carga)
                    db['fs.files'].update(
                        {'_id': linha['_id']},
                        {'$set': {'metadata.carga': dados_carga}}
                    )
                acum += 1
            else:
                if force_update:
                    db['fs.files'].update(
                        {'_id': linha['_id']},
                        {'$set': {'metadata.carga': 'NA'}}
                    )
    logger.info(' '.join([
        ' Resultado dados_carga_grava_fsfiles',
        ' Pesquisados', str(total),
        'Encontrados', str(acum),
        'Menor data', str(start),
        'Maior data', str(end)
    ]))
    return acum


def nlinhas_zip_dir(path):
    """Retorna o número de linhas de um conjunto de extrações.

    Dado um diretório(path) procura extrações do Siscomex CARGA, abre seus zip,
    abre o arquivo de cada zip e conta as linhas.

    Para AUDITORIA após um arquivamento da base CARGA confirmar se número de
    registros no arquivo (MongoDB) é igual a número original de linhas.

    Args:
        path: caminho do(s) arquivo(s) de extração(ões)

    Returns:
        Número de conhecimentos

    Comparar com:
        db['fs.files'].find({'metadata.carga.atracacao.dataatracacao':
                                {$gt: date, $lt: date}}).count()

    """
    contador = Counter()
    for zip_file in os.listdir(path):
        with ZipFile(os.path.join(path, zip_file)) as myzip:
            info_list = myzip.infolist()
            # print('info_list ',info_list)
            for info in info_list:
                if info.filename.find('0.txt') != -1:
                    with myzip.open(info) as txt_file:
                        txt_io = io.TextIOWrapper(
                            txt_file,
                            encoding=ENCODE, newline=''
                        )
                        reader = csv.reader(txt_io, delimiter='\t')
                        linha = next(reader)
                        print(info.filename)
                        print(linha)
                        tabela = linha[0]
                        nlines = sum(1 for linha in reader)
                    contador[tabela + ' - ' + zip_file] = nlines
                    contador[tabela] += nlines
    return OrderedDict(sorted(contador.items()))


if __name__ == '__main__':
    ZIP_DIR_TEST = os.path.join(os.path.dirname(__file__),
                                '..', '..', '..', '..', 'files', 'CARGA')
    pprint.pprint(nlinhas_zip_dir(ZIP_DIR_TEST))
