from virasana.integracao import dict_to_html

from virasana.utils.image_search import ImageSearch
import time

teste = {'Campo 1': 'Valor 1', 'Campo 2': 'Valor 2'}
print(dict_to_html(teste))


def print_elapsed(s0):
    elapsed = time.time() - s0
    print('Decorridos %fs' % (elapsed))
    return elapsed


s0 = time.time()
print('Loading numpy arrays...')
imgsearcher = ImageSearch()
print_elapsed(s0)
print('Registros:', imgsearcher.image_indexes.shape)

_id1 = imgsearcher.ids_indexes[1]
_id2 = imgsearcher.ids_indexes[100000]

s0 = time.time()
imgsearcher.get_list(_id1)
elapsed = print_elapsed(s0)
s0 = time.time()
imgsearcher.get_list(_id2)
print_elapsed(s0)

for i in range(4):
    s0 = time.time()
    imgsearcher.get_list(_id1)
    elapsed2 = print_elapsed(s0)
    assert elapsed2 < elapsed / 10
    s0 = time.time()
    imgsearcher.get_list(_id2)
    print_elapsed(s0)


for i in range(4):
    s0 = time.time()
    imgsearcher.get_chunk(_id1, i * 100)
    elapsed2 = print_elapsed(s0)
    assert elapsed2 < elapsed / 10
    s0 = time.time()
    imgsearcher.get_chunk(_id2, i * 100)
    print_elapsed(s0)
