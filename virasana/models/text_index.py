from collections import OrderedDict

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer


class TextSearch:

    def __init__(self, db):
        self.db = db
        cursor = db.fs.files.find(
            {'metadata.carga.conhecimento.identificacaoembarcador':
                 {'$exists': True, '$ne': None}},
            ['metadata.carga.conhecimento.identificacaoembarcador',
             'metadata.carga.conhecimento.descricaomercadoria']

        )  # .limit(20000)
        print('Consultando...')
        self.embarcadores = OrderedDict()
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
                identificacao = conhecimentos['identificacaoembarcador']
            self.embarcadores[line['_id']] = identificacao
        print('Vetorizando...')
        self.count_vect = CountVectorizer()
        word_count_vector = self.count_vect.fit_transform(
            self.embarcadores.values())
        print('Qtde palavras:', len(self.count_vect.vocabulary_))
        tfidf_transformer = TfidfTransformer(smooth_idf=True, use_idf=True)
        self.word_tfidf = tfidf_transformer.fit_transform(word_count_vector)

    def get_palavras_como(self, como: str):
        palavras_filtro = [
            item for item in self.count_vect.vocabulary_
            if item[:len(como)] == como
        ]
        return palavras_filtro


if __name__ == "__main__":
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para busca de texto')

    text_search = TextSearch(db)
    letras = input('Digite as primeiras letras de palavra a procurar:')
    print(text_search.get_palavras_como(letras))

    palavra = input()
    index_word = text_search.count_vect.vocabulary_.get(palavra)
    col = text_search.word_tfidf.getcol(index_word)
    # print(col)
    dados = col.nonzero()[0]
    rank = col.data
    print(col.nonzero())
    print(col.data)
    print(col.data.argsort())
    for ind in col.data.argsort()[::-1]:
        print(dados[ind], rank[ind])
    #    print(item)
    #    print(list(embarcadores.values())[item])
