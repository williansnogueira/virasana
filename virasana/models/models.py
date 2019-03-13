from datetime import datetime

import pymongo
from bson.objectid import ObjectId

CHAVES_MODELS = [
    'metadata.tags',
    'metadata.ocorrencias',
]


def create_indexes(db):
    """Utilitário. Cria índices relacionados aos modelos."""
    for chave in CHAVES_MODELS:
        try:
            db['fs.files'].create_index(chave)
        except pymongo.errors.OperationFailure:
            pass


class Ocorrencias():
    def __init__(self, db):
        self._db = db

    def add(self, _id, usuario, texto):
        self._db['fs.files'].update_one(
            {'_id': ObjectId(_id)},
            {'$addToSet':
                 {'metadata.ocorrencias':
                      {'id_ocorrencia': ObjectId(),
                       'usuario': usuario,
                       'texto': texto,
                       'data': datetime.now()
                       }
                  }
             }
        )
        return True

    def list(self, _id):
        imagem = self._db['fs.files'].find_one({'_id': ObjectId(_id)})
        ocorrencias = []
        if imagem:
            for ocorrencia in imagem['metadata'].get('ocorrencias', []):
                try:
                    ldata = datetime.strftime(ocorrencia['data'],
                                              '%d/%m/%Y %H:%M')
                except (ValueError, TypeError):
                    ldata = ''
                ocorrencias.append(
                    {'id_ocorrencia': str(ocorrencia.get('id_ocorrencia')),
                     'usuario': ocorrencia['usuario'],
                     'texto': ocorrencia['texto'],
                     'data': ldata}
                )
        return ocorrencias

    def list_usuario(self, _id, usuario):
        ocorrencias = self.list(_id)
        # print('###', ocorrencias)
        if ocorrencias and len(ocorrencias) > 0:
            ocorrencias = [ocorrencia for ocorrencia in ocorrencias
                           if ocorrencia['usuario'] == usuario]
        return ocorrencias

    def delete(self, _id, id_ocorrencia):
        imagem = self._db['fs.files'].find_one({'_id': ObjectId(_id)})
        if not imagem:
            return False
        ocorrencias = imagem['metadata']['ocorrencias']
        copy_ocorrencias = \
            [ocorrencia for ocorrencia in ocorrencias
             if (str(ocorrencia.get('id_ocorrencia')) == id_ocorrencia)]
        for uma_ocorrencia in copy_ocorrencias:
            self._db['fs.files'].update_one(
                {'_id': ObjectId(_id)},
                {'$pull':
                     {'metadata.ocorrencias': uma_ocorrencia}
                 }
            )
        return True


class Tags():

    hardcoded_tags = {
        '0': ' Selecione tag desejada',
        '1': 'Cocaína',
        '2': 'Armas',
        '3': 'Auditando',
        '4': 'Seleção de Risco',
        '5': 'Erro de predição - detecção contêiner',
        '6': 'Erro de predição - vazio',
        '7': 'Erro de predição - peso',
        '8': 'Erro de predição - outro'
    }

    def mount_tags(self):
        """Para evitar a criação desmesurada de tags elas serão centralizadas.

        Aqui, se a tabela não existir no banco, cria algumas hard_coded.
        Depois, o administrador poderá criar novas no BD.
        """
        cursor = self._db['Tags'].find()
        tags = list(cursor)
        if len(tags) == 0:
            # Se não existe tabela, cria, preenche e chama de novo mesmo método
            for id, descricao in self.hardcoded_tags.items():
                self._db['Tags'].insert_one({'id': id,
                                       'descricao': descricao})
            return self.mount_tags()
        tags_text = []
        for row in tags:
            id = row['id']
            descricao = row['descricao']
            self.dict_tags[id] = descricao
            tags_text.append((id, id + '- ' + descricao))
        self.tags_text = sorted(tags_text)

    def __init__(self, db):
        self._db = db
        self.dict_tags = {}
        self.tags_text = []
        self.mount_tags()

    def add(self, _id, usuario, tag):
        self._db['fs.files'].update_one(
            {'_id': ObjectId(_id)},
            {'$addToSet':
                 {'metadata.tags':
                      {'usuario': usuario,
                       'tag': tag
                       }
                  }
             }
        )
        return True

    def list(self, _id):
        imagem = self._db['fs.files'].find_one({'_id': ObjectId(_id)})
        if not imagem:
            return None
        # print(imagem)
        tags = []
        for tag in imagem['metadata'].get('tags', []):
            tags.append(
                {'usuario': tag['usuario'],
                 'tag': tag['tag'],
                 'descricao': self.dict_tags.get(tag['tag'], '')
                 }
            )
        return tags

    def list_usuario(self, _id, usuario):
        tags = self.list(_id)
        # print('###', tags)
        if tags and len(tags) > 0:
            tags = [tag for tag in tags if tag['usuario'] == usuario]
        return tags

    def delete(self, _id, usuario, tag):
        imagem = self._db['fs.files'].find_one({'_id': ObjectId(_id)})
        if imagem is None:
            return
        delete_tags = [atag for atag in imagem['metadata']['tags']
                       if (atag['usuario'] == usuario and
                           atag['tag'] == tag)]
        print(delete_tags)
        for uma_tag in delete_tags:
            self._db['fs.files'].update_one(
                {'_id': ObjectId(_id)},
                {'$pull':
                     {'metadata.tags': uma_tag}
                 }
            )
        return True

    def tagged(self, usuario=None, tag=None, limit=2000):
        if tag is None and usuario is None:
            raise ValueError('Usuário e tag não informados.'
                             'Ao menos um dos dois é obrigatório.')
        find_expression = {}
        if usuario is not None:
            find_expression['usuario'] = usuario
        if tag is not None:
            find_expression['tag'] = tag
        if (usuario is None or tag is None):
            find_expression = {'$elemMatch': find_expression}
        # print(find_expression)
        cursor = self._db['fs.files'].find(
            {'metadata.tags': find_expression},
            limit=limit
        )
        return cursor


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para Modelos')
    create_indexes(db)
