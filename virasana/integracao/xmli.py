"""Funções para leitura e tratamento arquivos XML criados pelos escâneres."""
from datetime import datetime
import sys

import chardet
from gridfs import GridFS

# from ajna_commons.conf import ENCODE
from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import sanitizar, unicode_sanitizar

if sys.platform == 'win32':
    import lxml.etree as ET
else:
    # defusedxml under 5.0 pode ser incompatível com python3.6
    # if int(sys.version_info[1]) > 5:
    #    import lxml.etree as ET
    # else:
    import defusedxml.ElementTree as ET


FALTANTES = {'metadata.xml.date': None,
             'metadata.contentType': 'image/jpeg'
             }

FIELDS = ('TruckId', 'Site', 'Date', 'PlateNumber', 'IsContainerEmpty',
          'Login', 'Workstation', 'UpdateDateTime', 'ClearImgCount',
          'UpdateCount', 'LastStateDateTime')

# Fields to be converted to ISODate
DATE_FIELDS = ('Date', 'UpdateDateTime', 'LastStateDateTime')

DATA = 'metadata.xml.date'


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração."""
    db['fs.files'].create_index('metadata.xml.container')
    db['fs.files'].create_index('metadata.xml.alerta')
    for field in FIELDS:
        db['fs.files'].create_index('metadata.xml.' + field.lower())


def xml_todict(xml) -> dict:
    """Recebe um XML string stream, retorna um dict com campos do XML.

    Lê o XML que acompanha as imagens no formato do Escâner da ALFSTS,
    retorna dict(s) com os dados considerados mais importantes e resumos
    (ex.: número do(s) cc(s), datas, etc) - configurar na var. fields

    Args:
        xml: string de texto na memória com o conteúdo do arquivo XML

    Returns:
        Dicionário com as tags selecionadas. A tag container é uma lista
        (imagens de cc de 20' podem conter dois contêineres escaneados)

    """
    result = {}
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as err:
        print('*****************XML**********************')
        print(xml)
        print('*****************XML END')
        xml = unicode_sanitizar(xml)
        # logger.error(err, exc_info=True)
        try:
            root = ET.fromstring(xml)
        except ET.ParseError as err:
            print('*****************XML**********************')
            print(xml)
            print('*****************XML END')
            print(str(err))
            return result
            # raise ET.ParseError('XML intratável??')

            # return result
    alerta = xml.find('>al<') == -1
    result['alerta'] = alerta
    for field in FIELDS:
        for tag in root.iter(field):
            text = ''
            if tag.text:
                text = sanitizar(tag.text)
            if field in DATE_FIELDS:
                try:
                    text = text.split('.')[0]
                    text = datetime.strptime(text, '%Y-%m-%dt%H:%M:%S')
                except ValueError as err:
                    pass
            result[field.lower()] = text
    lista_conteineres = []
    for tag in root.iter('ContainerId'):
        numero = tag.text
        if numero is not None:
            numero = numero.replace('?', 'X')
            lista_conteineres.append(numero.casefold())
    result['container'] = lista_conteineres
    return result


def dados_xml_grava_fsfiles(db, batch_size=5000,
                            data_inicio=datetime(1900, 1, 1),
                            update=True):
    """Busca por registros no GridFS sem info do XML.

    Busca por registros no fs.files (GridFS - imagens) que não tenham metadata
    importada do arquivo XML. Itera estes registros, consultando a
    xml_todict para ver se retorna informações do XML. Encontrando
    estas informações, grava no campo metadata.xml do fs.files

    Args:
        db: connection to mongo with database setted

        batch_size: número de registros a consultar/atualizar por chamada

        data_inicio: filtra por data de escaneamento maior que a informada

        update: Caso seja setado como False, apenas faz consulta, sem
            atualizar metadata da collection fs.files

    Returns:
        Número de registros encontrados

    """
    file_cursor = db['fs.files'].find(
        {'metadata.xml': None,
         'metadata.dataescaneamento': {'$gt': data_inicio},
         'metadata.contentType': 'image/jpeg'
         }).limit(batch_size)
    fs = GridFS(db)
    total = file_cursor.count()
    acum = 0
    for linha in file_cursor:
        filename = linha.get('filename')
        if not filename:
            continue
        xml_filename = filename[:-11] + '.xml'
        xml_document = db['fs.files'].find_one({'filename': xml_filename})
        if not xml_document:
            print(xml_filename, ' não encontrado')
            continue
        file_id = xml_document.get('_id')
        if not fs.exists(file_id):
            continue
        raw = fs.get(file_id).read()
        encode = chardet.detect(raw)
        # print(encode)
        encoding = [encode['encoding'],
                    'latin1',
                    'utf8',
                    'ascii',
                    'windows-1250',
                    'windows-1252']
        dados_xml = {}
        for e in encoding:
            try:
                xml = raw.decode(e)
                # TODO: see why sometimes a weird character
                # appears in front of content
                posi = xml.find('<DataForm>')
                # print('POSI', posi)
                if posi == -1:
                    xml = xml[2:]
                else:
                    xml = xml[posi:]
                ET.fromstring(xml)
                dados_xml = xml_todict(xml)
                break
            except Exception as err:
                print('Erro de encoding', e, err)
        if dados_xml != {}:
            if update:
                db['fs.files'].update(
                    {'_id': linha['_id']},
                    {'$set': {'metadata.xml': dados_xml}}
                )
            acum += 1
    logger.info(' '.join([
        'Resultado dados_xml_grava_fsfiles',
        'Pesquisados', str(total),
        'Encontrados', str(acum)
    ]))
    return acum
