"""
For tests with PyMongo and MongoDB.

Tests for sintax and operations before putting into main code

"""
import pprint
import typing
import timeit
from datetime import datetime, timedelta
from pymongo import MongoClient

db = MongoClient()['test']

# CARGA - teste para checar como pegar Info do Contêiner na Base CARGA


def mongo_find_in(db, collection: str, field: str, in_set: set,
                  set_field: str=None) -> typing.Tuple[list, set]:
    """Perform a find $in in_set on db.collection.

    Args:
        db: connection to mongo with database setted
        collection: name of mongo collection to query
        field: field to filter by
        in_set: list of values to pass to $in operator
        result_field: field to retrieve unique values (optional)

    Returns:
        dictionary of results, formated key:value (Only not null fields)
        set of set_field

    """
    result = []
    field_set = set()
    filtro = {field: {'$in': list(in_set)}}
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


def busca_atracacao_data(atracacoes: list,  scan_datetime: datetime,
                         threshold=None) -> int:
    """Pega da lista de atracacoes a atracção com a data mais próxima.

    Args:
        atracacoes: lista de dict contendo os registros das atracacoes
        data: data buscada
        threshold: máxima diferença entre as datas - default definido no
            começo da função
    Returns:
        índice da atracação, None se atracação não existe ou não está no
        intervalo threshold

    """
    index = None
    if threshold is None:
        threshold = timedelta(days=2)
    for ind, atracacao in enumerate(atracacoes):
        data = atracacao['dataatracacao']
        hora = atracacao['horaatracacao']
        datahora = datetime.strptime(data+' '+hora, '%d/%m/%Y %H:%M:%S')
        datetimedelta = abs(datahora - scan_datetime)
        # print(datetimedelta, threshold)
        if datetimedelta < threshold:
            threshold = datetimedelta
            index = ind
    return index


def busca_info_container(numero: str, data_escaneamento: datetime) -> dict:
    """Busca heurística na base CARGA MongoDB de dados sobre o Contêiner.

    A busca é baseada na data de escaneamento. O threshold (diferença aceita
    entre a data de atracação e escaneamento), por padrão, é de 4 dias.
    Dentro destes 4 dias, será considerado o CE/Manifesto/Escala com menor
    diferença de data como o pertencente a este contêiner.
    Note-se que o resultado não é garantido, podendo trazer um CE incorreto.
    As informações são imperfeitas e não há como garantir trazer o CE correto,
    mas espera-se um acerto próximo de 100%, já que a frequência de cada
    contêiner em cada porto tem um intervalo típico de semanas e até meses,
    sendo extremamente incomum um contêiner ter duas "viagens" no mesmo
    porto em menos de 4 dias +/-
    Args:
        numero: número completo do contêiner
        data_escaneamento: data e hora do escaneamento, conforme
        arquivo XML original do escâner
    Returns:
        json_dict: dict com campos e valores de informações da base CARGA
        VAZIO se não encontrar nada dentro do threshold
        (Caso não encontre atracacao para o Contêiner no prazo, o dado ?ainda?
        não existe ou não foi importado ou há um erro)!
    """
    json_dict = {}
    # Primeiro busca por contêiner vazio (dez vezes mais rápido)
    containeres_vazios, manifestos_vazios_set = mongo_find_in(
        db, 'CARGA.ContainerVazio', 'container', set([numero]), 'manifesto')
    escalas_vazios, escalas_vazios_set = mongo_find_in(
        db, 'CARGA.EscalaManifesto', 'manifesto', manifestos_vazios_set, 'escala')
    atracacoes_vazios, _ = mongo_find_in(
        db, 'CARGA.AtracDesatracEscala', 'escala', escalas_vazios_set)
    index_atracacao = busca_atracacao_data(
        atracacoes_vazios, data_escaneamento)
    if index_atracacao is not None:
        atracacao = atracacoes_vazios[index_atracacao]
        json_dict['vazio'] = True
        json_dict['atracacao'] = atracacao
        manifesto = [linha['manifesto'] for linha in escalas_vazios
                     if linha['escala'] == atracacao['escala']]
        json_dict['manifesto'] = mongo_find_in(
            db, 'CARGA.Manifesto', 'manifesto', manifesto)
        # print(atracacao)
        json_dict['container'] = [linha for linha in containeres_vazios
                                  if linha['manifesto'] == manifesto[0]]
        return json_dict
    # else:
    # Não achou atracacao para o Contêiner no prazo. Verificar se Contêiner é cheio
    containeres, conhecimentos_set = mongo_find_in(
        db, 'CARGA.Container', 'container', set([container]), 'conhecimento')
    conhecimentos, _ = mongo_find_in(
        db, 'CARGA.Conhecimento', 'conhecimento', conhecimentos_set)
    manifestos, manifestos_set = mongo_find_in(
        db, 'CARGA.ManifestoConhecimento', 'conhecimento',
        conhecimentos_set, 'manifesto')
    escalas, escalas_set = mongo_find_in(
        db, 'CARGA.EscalaManifesto', 'manifesto', manifestos_set, 'escala')
    atracacoes, _ = mongo_find_in(
        db, 'CARGA.AtracDesatracEscala', 'escala', escalas_set)
    index_atracacao = busca_atracacao_data(atracacoes, data_escaneamento)
    if index_atracacao is not None:
        # Agora sabemos o número do(s) CE(s) corretos do contêiner
        # Basta montar uma estrutura de dados com as informações
        atracacao = atracacoes[index_atracacao]
        json_dict['vazio'] = False
        json_dict['atracacao'] = atracacao
        manifesto = [linha['manifesto'] for linha in escalas
                     if linha['escala'] == atracacao['escala']]
        json_dict['manifesto'] = mongo_find_in(
            db, 'CARGA.Manifesto', 'manifesto', manifesto)
        conhecimentos = [linha['conhecimento'] for linha in manifestos
                         if linha['manifesto'] == manifesto[0]]
        json_dict['conhecimento'] = mongo_find_in(
            db, 'CARGA.Conhecimento', 'conhecimento', conhecimentos)
        json_dict['ncm'] = mongo_find_in(
            db, 'CARGA.NCM', 'conhecimento', conhecimentos)
        json_dict['container'] = [linha for linha in containeres
                                  if linha['conhecimento'] in conhecimentos]

    return json_dict


container = 'tclu2967718'
container_vazio = 'apru5774515'
data_escaneamento_false = datetime.utcnow()
data_escaneamento_true = datetime.strptime('17-08-02', '%y-%m-%d')
# Teste de desempenho
"""reps = 10
tempo = timeit.timeit(
    stmt='busca_info_container(container_vazio, data_escaneamento_false)',
    number=reps, globals=globals())
print('loops(TUDO - data falsa):', reps,
      'total time:', tempo, 'per loop:', tempo/reps)
tempo = timeit.timeit(
    stmt='busca_info_container(container, data_escaneamento_true)',
    number=reps, globals=globals())
print('loops(cheio):', reps, 'total time:', tempo, 'per loop:', tempo/reps)
tempo = timeit.timeit(
    stmt='busca_info_container(container_vazio, data_escaneamento_true)',
    number=reps, globals=globals())
print('loops(vazio):', reps, 'total time:', tempo, 'per loop:', tempo/reps)
"""
# teste de função
assert busca_info_container(container, data_escaneamento_false) == {}
assert busca_info_container(container, data_escaneamento_true) != {}
assert busca_info_container(container_vazio, data_escaneamento_false) == {}
assert busca_info_container(container_vazio, data_escaneamento_true) != {}


print('Cheio')
pprint.pprint(busca_info_container(container, data_escaneamento_true))

print('Vazio')
pprint.pprint(busca_info_container(container_vazio, data_escaneamento_true))

file_cursor = db['fs.files'].find({'metadata.CARGA': None})
count = file_cursor.count()
print('Files count with no CARGA metadata', count)

# Teste com dados reais
acum = 0
start = datetime.utcnow()
end = start - timedelta(days=1000)
for linha in file_cursor:
    container = linha.get('metadata.numeroinformado')
    data = linha.get('metadata.dataimportacao')
    if data < start:
        start = data
    if data > end:
        end = data
    if busca_info_container(container, data) != {}:
        acum +=1
print(acum, start, end)