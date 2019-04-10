from math import log10
from collections import OrderedDict
from datetime import datetime

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer


class TextSearch:

    def __init__(self, db):
        self.db = db
        self.filter = {
            'metadata.carga.conhecimento.identificacaoembarcador':
                {'$exists': True, '$ne': None}
        }
        self.projection = [
            'metadata.carga.conhecimento.identificacaoembarcador',
            'metadata.carga.conhecimento.descricaomercadoria'
        ]
        cursor = db.fs.files.find(
            self.filter, self.projection
        )  # .limit(20000)
        print('Consultando...')
        self.documentos = OrderedDict()
        for line in cursor:
            # print(line)
            conhecimentos = line['metadata']['carga']['conhecimento']
            if isinstance(conhecimentos, list):
                identificacao = ' '.join([
                    c['identificacaoembarcador'] + ' ' +
                    c['descricaomercadoria']
                    for c in conhecimentos]
                )
            elif isinstance(conhecimentos, str):
                identificacao = conhecimentos['identificacaoembarcador'] + \
                                ' ' + conhecimentos['descricaomercadoria']
            self.documentos[line['_id']] = identificacao
        print('Vetorizando...')
        self.count_vect = CountVectorizer()
        word_count_vector = self.count_vect.fit_transform(
            self.documentos.values())
        print('Qtde palavras:', len(self.count_vect.vocabulary_))
        tfidf_transformer = TfidfTransformer(smooth_idf=True, use_idf=True)
        self.word_tfidf = tfidf_transformer.fit_transform(word_count_vector)

    def get_palavras_como(self, como: str):
        palavras_filtro = [
            item for item in self.count_vect.vocabulary_
            if item[:len(como)] == como
        ]
        return sorted(palavras_filtro)

    def get_documentos_palavras(self, palavras: list):
        results = {}
        for palavra in palavras:
            index_word = self.count_vect.vocabulary_.get(palavra)
            if index_word:
                col = self.word_tfidf.getcol(index_word)
                doc_indexes = col.nonzero()[0]
                for doc_index, rank in zip(doc_indexes, col.data):
                    points = results.get(doc_index, 0)
                    results[doc_index] = points + rank
                #    print(item)
                #    print(list(embarcadores.values())[item])
        return sorted(results.items(), key=lambda kv: kv[1], reverse=True)

    def get_documentos_frase(self, frase: str):
        palavras = frase.split(' ')
        palavras = [palavra.strip() for palavra in palavras]
        print(palavras)
        return self.get_documentos_palavras(palavras)

    def get_documentoids(self, lista_documentos: list):
        lista_ids = list(self.documentos.keys())
        ids = [lista_ids[doc[0]] for doc in lista_documentos]
        return ids

    def get_documentoids_frase(self, frase: str):
        lista_ids = list(self.documentos.keys())
        lista_documentos = self.get_documentos_frase(frase)
        ids = [lista_ids[doc[0]] for doc in lista_documentos]
        ranks = [doc[1] for doc in lista_documentos]
        return ids, ranks

    def get_itens(self, _ids, ranks):
        itens = list()
        for _id, rank in zip(_ids, ranks):
            item = dict()
            fs_row = self.db.fs.files.find_one(
                {'_id': _id},
                self.projection + ['metadata.dataescaneamento'])
            dataescaneamento = fs_row['metadata'].get('dataescaneamento')
            conhecimentos = fs_row['metadata']['carga']['conhecimento']
            if isinstance(conhecimentos, list):
                identificacaoembarcador = ', '.join(
                    [c['identificacaoembarcador'] for c in conhecimentos]
                )
                descricaomercadoria = ', '.join(
                    [c['descricaomercadoria'] for c in conhecimentos]
                )
            elif isinstance(conhecimentos, str):
                identificacaoembarcador = conhecimentos['identificacaoembarcador']
                descricaomercadoria = conhecimentos['descricaomercadoria']

            item['_id'] = str(_id)
            item['identificacaoembarcador'] = identificacaoembarcador
            item['descricaomercadoria'] = descricaomercadoria
            item['dataescaneamento'] = datetime.strftime(
                dataescaneamento, '%d/%m/%Y %H:%M')
            item['rank'] = rank
            date_months_diff = \
                (datetime.today().year - dataescaneamento.year) * 12 + \
                 datetime.today().month - dataescaneamento.month
            timed_rank = rank
            if date_months_diff > 0:
                timed_rank = rank / log10(10 + date_months_diff)
            item['timed_rank'] = timed_rank
            itens.append(item)
        return sorted(itens, key = lambda item: item['timed_rank'], reverse=True)

    def get_itens_frase(self, frase):
        _ids, ranks = self.get_documentoids_frase(frase)
        return self.get_itens(_ids, ranks)


if __name__ == "__main__":
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para busca de texto')
    text_search = TextSearch(db)

    letras = input('Digite as primeiras letras de palavra a procurar: ')
    print(text_search.get_palavras_como(letras))

    frase = input('Digite palavras a procurar: ')
    docs = text_search.get_documentos_frase(frase)
    print(docs)
    print(text_search.get_documentoids(docs))
    _ids, ranks = text_search.get_documentoids_frase(frase)
    print(_ids, ranks)
    print(text_search.get_itens(_ids, ranks))
