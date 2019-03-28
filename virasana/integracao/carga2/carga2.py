from collections import defaultdict, OrderedDict
from datetime import date, datetime, time, timedelta

def carga_faltantes(data_inicio, data_fim, campo):
    dict_faltantes = OrderedDict()
    filtro = FALTANTES
    data_fim = datetime.combine(data_fim, time.max)  # Pega atá a última hora do dia
    filtro['metadata.dataescaneamento'] = {'$gte': data_inicio, '$lte': data_fim}
    projection = ['metadata.numeroinformado', 'metadata.dataescaneamento']
    print(filtro)
    fs_cursor = db['fs.files'].find(filtro, projection=projection).sort('metadata.numeroinformado')
    for linha in fs_cursor:
        numero = linha['metadata']['numeroinformado'].lower()
        dict_faltantes[numero] = linha['_id']
    return dict_faltantes

campo = 'manifesto'
start = datetime(2017, 7, 5)
end = datetime(2017, 7, 15)
dict_faltantes = carga_faltantes(start, end, campo)
total = len(dict_faltantes.keys())
print('Total de contâineres sem %s de %s a %s: %s' %  (campo, start, end, total))



def carga_grava_fsfiles(db, data_inicio, data_fim):
    """Busca por registros no GridFS sem info da Pesagem

    Busca por registros no fs.files (GridFS - imagens) que não tenham metadata
    importada da pesagem.

    Args:
        db: connection to mongo with database setted
        batch_size: número de registros a consultar/atualizar por chamada
        data_inicio: filtra por data de escaneamento maior que a informada

    Returns:
        Número de registros encontrados

    """
    filtro = FALTANTES
    DELTA_VAZIO = 5
    DELTA_IMPORTACAO = -5
    DELTA_EXPORTACAO = +10
    data_fim = data_fim + timedelta(hours=1, minutes=59, seconds=59)  # Pega atá a última hora do dia
    filtro['metadata.dataescaneamento'] = {'$gte': data_inicio, '$lte': data_fim}
    projection = ['metadata.numeroinformado', 'metadata.dataescaneamento']
    total = db['fs.files'].count_documents(filtro)
    fs_cursor = list(
        db['fs.files'].find(filtro, projection=projection).sort('metadata.numeroinformado')
    )
    manifestos_vazio = list(
        db['CARGA.AtracDesatracEscala'].find(
            {'dataatracacaoiso': {'$gte': data_inicio - timedelta(days=DELTA_VAZIO),
                                  '$lte': data_fim + timedelta(days=DELTA_VAZIO)},
             'codigoconteinerentrada': {'$exists': True, '$ne': None, '$ne': ''}}
        ).sort('codigoconteinerentrada')
    )
    acum = 0
    logger.info(
        'Processando Manifestos de Vazio para imagens de %s a %s. '
        'Pesquisando Manifestos %s dias antes e depois. '
        'Imagens encontradas sem info CARGA: %s  Manifestos encontrados %s.'
        % (data_inicio, data_fim, DELTA_VAZIO, len(fs_cursor), len(manifestos_vazio))
    )
    linhas_manifesto = compara_conteineres(fs_cursor, manifestos_vazio, 'conteiner')
    # acum = len(linhas_entrada) + len(linhas_saida)
    logger.info(
        'Resultado pesagens_grava_fsfiles '
        'Pesquisados %s. '
        'Encontrados %s .'
        % (total, len(linhas_manifesto))
    )
    acum = 0
    acum += insere_fsfiles(db, linhas_manifesto, 'entrada')
    return acum



if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para CARGA')
    create_indexes(db)