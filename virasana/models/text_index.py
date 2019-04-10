from collections import OrderedDict

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.pipeline import Pipeline

pipeline = Pipeline([
    ('vect', CountVectorizer()),
    ('tfidf', TfidfTransformer()),
])

if __name__ == "__main__":
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para busca de texto')

    cursor = db.fs.files.find(
        {'metadata.carga.conhecimento.identificacaoembarcador':
             {'$exists': True, '$ne': None}},
        ['metadata.carga.conhecimento.identificacaoembarcador']
    )  # .limit(20000)

    print('Consultando...')

    embarcadores = OrderedDict()
    for line in cursor:
        # print(line)
        conhecimentos = line['metadata']['carga']['conhecimento']
        if isinstance(conhecimentos, list):
            identificacao = ' '.join(
                [c['identificacaoembarcador'] for c in conhecimentos]
            )
        elif isinstance(conhecimentos, str):
            identificacao = conhecimentos['identificacaoembarcador']
        embarcadores[line['_id']] = identificacao

    print('Vetorizando...')

    # print(embarcadores)
    count_vect = CountVectorizer()
    word_count_vector = count_vect.fit_transform(embarcadores.values())
    print(word_count_vector.shape)

    print('Qtde palavras:', len(count_vect.vocabulary_))
    tfidf_transformer = TfidfTransformer(smooth_idf=True, use_idf=True)
    word_tfidf = tfidf_transformer.fit_transform(word_count_vector)

    letras = input()
    palavras_filtro = [item for item in count_vect.vocabulary_
                       if item[:len(letras)] == letras]
    print(palavras_filtro)

    palavra = input()
    index_word = count_vect.vocabulary_.get(palavra)
    col = word_tfidf.getcol(index_word)
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
