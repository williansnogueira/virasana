from bson.objectid import ObjectId


class Ocorrencias():
    def __init__(self, db):
        self._db = db

    def add(self, _id, usuario, texto):
        self._db['fs.files'].update_one(
            {'_id': ObjectId(_id)},
            {'$addToSet':
                 {'metadata.ocorrencias':
                      {'usuario': usuario,
                       'texto': texto
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
        return imagem['metadata']['ocorrencias']

    def list_usuario(self, _id, usuario):
        ocorrencias = self.list(_id)
        # print('###', ocorrencias)
        if ocorrencias and len(ocorrencias) > 0:
            ocorrencias = [ocorrencia for ocorrencia in ocorrencias if ocorrencia['usuario'] == usuario]
        return ocorrencias

    def delete(self, _id, usuario, texto):
        ocorrencias = self.list(_id)
        copy_ocorrencias = [ocorrencia for ocorrencia in ocorrencias
                            if (ocorrencia['usuario'] == usuario and
                                ocorrencia['texto'] == texto)]
        for uma_ocorrencia in copy_ocorrencias:
            self._db['fs.files'].update_one(
                {'_id': ObjectId(_id)},
                {'$pull':
                     {'metadata.ocorrencias': uma_ocorrencia}
                 }
            )
        return True


class Tags():
    def __init__(self, db):
        self._db = db

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
        return imagem['metadata']['tags']

    def list_usuario(self, _id, usuario):
        tags = self.list(_id)
        # print('###', tags)
        if tags and len(tags) > 0:
            tags = [tag for tag in tags if tag['usuario'] == usuario]
        return tags

    def delete(self, _id, usuario, tag):
        tags = self.list(_id)
        copy_tags = [atag for atag in tags
                     if (atag['usuario'] == usuario and
                         atag['tag'] == tag)]
        for uma_tag in copy_tags:
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
        if not usuario is None:
            find_expression['usuario'] = usuario
        if not tag is None:
            find_expression['tag'] = tag
        if (usuario is None or tag is None):
            find_expression = {'$elemMatch': find_expression}
        # print(find_expression)
        cursor = self._db['fs.files'].find(
            {'metadata.tags': find_expression},
            limit=limit
        )
        return cursor
