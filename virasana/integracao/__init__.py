"""
Funções padrão para exploração do GridFS.

"""
from virasana.integracao import carga
from virasana.integracao import xml

IMAGENS = {'metadata.contentType': 'image/jpeg'}


def gridfs_count(db, filtro):
    return db['fs.files'].find(filtro).count()


def stats_resumo(db):
    filtro = IMAGENS
    stats = {}
    total = gridfs_count(db, filtro)
    stats['total'] = total
    filtro = carga.FALTANTES
    stats['carga'] = total - gridfs_count(db, filtro)
    filtro = xml.FALTANTES
    stats['xml'] = total - gridfs_count(db, filtro)
    return stats


def plot_stats(db):
    pass


def stats_por_XXX(db):
    pass
