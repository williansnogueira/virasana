"""Funções utilitárias para exportação e montagem de training sets."""


def campos_mongo_para_lista(db, filtro: dict,
                            chaves: list,
                            limit=0)-> list:
    """Consulta MongoDB retornando lista.

    Args:
        db: conexão com MongoDB
        filtro:filtro a aplicar na query
        chaves: campos que serão recuperados
        limit: número de registros a limitar consulta

    Returns:
        lista contendo nomes de campo na primeira linha e valores a seguir,
        no estilo de uma planilha/csv

    """
    cursor = db['fs.files'].find(filtro).limit(limit)
    lista = []
    caminhos = [campo.split('.') for campo in chaves]
    cabecalhos = [caminho[len(caminho) - 1] for caminho in caminhos]
    lista.append(cabecalhos)
    for linha in cursor:
        registro = []
        for caminho in caminhos:
            sub = linha[caminho[0]]
            for chave in caminho[1:]:
                if isinstance(sub, list):
                    sub = sub[0]
                if sub and isinstance(sub, dict):
                    sub = sub.get(chave)
            registro.append(sub)
        lista.append(registro)
    return lista
