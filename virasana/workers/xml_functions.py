"""Funções para leitura e tratamento arquivos XML gerados pelos equipamentos.
"""
import defusedxml.ElementTree as ET
from gridfs import GridFS

from ajna_commons.flask.log import logger


FALTANTES = {'metadata.xml': None,
             'metadata.contentType': 'image/jpeg'
             }


def xml_todict(xml) -> dict:
    """Recebe um XML string stream, retorna um dict com campos do XML.

    Lê o XML que acompanha as imagens no formato do Escâner da ALFSTS,
    retorna dict(s) com os dados considerados mais importantes e resumos
    (ex.: número do(s) cc(s), datas, etc) - configurar na var. fields
    Args:
        xml: string de texto na memória com o conteúdo do arquivo XML
    Returns:
        dicionário com as tags selecionadas. A tag container é uma lista
        (imagens de cc de 20' podem conter dois contêineres escaneados)
    """
    result = {}
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as err:
        logger.error(err, exc_info=True)
        logger.error(xml)
        return result
    fields = ('TruckId', 'Site', 'Date', 'PlateNumber', 'IsContainerEmpty',
              'Login', 'Workstation', 'UpdateDateTime', 'ClearImgCount',
              'UpdateCount', 'LastStateDateTime')
    for field in fields:
        for tag in root.iter(field):
            result[field] = tag.text
    lista_conteineres = []
    for tag in root.iter('ContainerId'):
        numero = tag.text
        if numero is not None:
            numero = numero.replace('?', 'X')
            lista_conteineres.append(numero)
    result['ContainerId'] = lista_conteineres
    return result


def dados_xml_grava_fsfiles(db, batch_size=100, data_inicio=0, update=True):
    """Busca por registros no GridFS sem info do XML

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
        número de registros encontrados
    """
    file_cursor = db['fs.files'].find(
        {'metadata.xml': None,
         'metadata.dataescaneamento': {'$gt': data_inicio},
         'metadata.contentType': 'image/jpeg'
         })
    fs = GridFS(db)
    acum = 0
    for linha in file_cursor.limit(batch_size):
        filename = linha.get('filename')
        if not filename:
            continue
        xml_filename = filename[:-11] + '.xml'
        xml_document = db['fs.files'].find_one({'filename': xml_filename})
        if not xml_document:
            continue
        file_id = xml_document.get('_id')
        if not fs.exists(file_id):
            continue
        grid_out = fs.get(file_id)
        # TODO: View this encode (Use ENCODE from conf??)
        xml = grid_out.read().decode('latin1')
        # TODO: see why sometimes a weird character appears in front of content
        posi = xml.find('<DataForm>')
        if posi == -1:
            xml = xml[2:]
        else:
            xml = xml[posi:]
        dados_xml = xml_todict(xml)
        if dados_xml != {}:
            if update:
                db['fs.files'].update(
                    {'_id': linha['_id']},
                    {'$set': {'metadata.xml': dados_xml}}
                )
            acum += 1
    logger.info(' '.join([
        'Resultado dados_xml_grava_fsfiles',
        'Pesquisados', str(batch_size),
        'Encontrados', str(acum)
    ]))
    return acum
