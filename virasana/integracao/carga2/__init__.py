from collections import OrderedDict
import datetime


FALTANTES = {'metadata.contentType': 'image/jpeg',
             'metadata.carga.atracacao.escala': None}


def carga_faltantes(db, data_inicio, data_fim, campo):
    dict_faltantes = OrderedDict()
    filtro = FALTANTES
    data_fim = datetime.datetime.combine(data_fim, datetime.time.max)  # Pega atá a última hora do dia
    filtro['metadata.dataescaneamento'] = {'$gte': data_inicio, '$lte': data_fim}
    projection = ['metadata.numeroinformado', 'metadata.dataescaneamento']
    # print(filtro)
    fs_cursor = db['fs.files'].find(filtro, projection=projection).sort('metadata.numeroinformado')
    for linha in fs_cursor:
        numero = linha['metadata']['numeroinformado'].lower()
        dict_faltantes[numero] = linha['_id']
    return dict_faltantes
