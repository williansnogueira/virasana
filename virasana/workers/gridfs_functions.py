"""
Funções padrão para exploração do GridFS.

"""
from virasana.workers import carga_functions
from virasana.workers import xml_functions


def gridfs_count(db, filtro):
    return db['fs.files'].find(filtro).count()


def stats_resumo(db):
    filtro = {}
    stats = {}
    total = gridfs_count(db, filtro)
    stats['total'] = total
    filtro = carga_functions.FALTANTES
    stats['carga'] = total - gridfs_count(db, filtro)
    filtro = xml_functions.FALTANTES
    stats['xml'] = total - gridfs_count(db, filtro)
    return stats


def plot_stats(db):
    pass


def stats_por_XXX(db):
    pass
