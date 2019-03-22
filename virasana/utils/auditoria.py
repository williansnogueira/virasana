"""Funções para Auditoria/comparação nos metadados de fs.files."""
from collections import defaultdict

from ajna_commons.flask.log import logger



class Auditoria():
    """Recebe params, monta consultas de auditoria entre campos fs.files."""

    FILTROS_AUDITORIA = {
        '1': {'filtro': {'metadata.carga.vazio': True,
                         'metadata.predictions.vazio': False},
              'order': [('metadata.predictions.peso', -1)],
              'descricao': 'Contêineres informados como vazios mas detectados ' +
                           'como não vazios (ordem decrescente de peso detectado)'
              },
        '2': {'filtro': {'metadata.carga.vazio': False,
                         'metadata.predictions.vazio': True},
              'order': [('metadata.predictions.peso', 1)],
              'descricao': 'Contêineres informados como contendo carga mas ' +
                           'detectados como vazios (ordem de peso detectado)'
              },
        '3': {'filtro': {'metadata.alertapeso': True},
              'order': [('metadata.diferencapeso', 1)],
              'descricao': 'Contêineres com maiores divergências de peso'
              },
        '4': {'filtro': {'metadata.contentType': 'image/jpeg',
                         'metadata.predictions.bbox': {'$exists': False}},
              'order': [('metadata.dataescaneamento', 1)],
              'descricao': 'Imagens sem contêiner detectado'
              }
    }

    def __init__(self, db):
        """Init.

        Args:
            db: conexão ao MongoDB a ser usada
        """
        self.db = db
        self.relatorios = {}
        self.dict_auditoria = {}
        self.mount_filtros()

    def mount_filtros(self):
        """Para evitar a criação desmesurada de filtros eles serão centralizados.

        Aqui, se a tabela não existir no banco, cria algumas hard_coded.
        Depois, o administrador poderá criar novas no BD.
        """
        cursor = self._db['Auditorias'].find()
        auditorias = list(cursor)
        if len(auditorias) == 0:
            logger.debug('Criando tabela Auditorias...')
            # Se não existe tabela, cria, preenche e chama de novo mesmo método
            for id, campos in self.FILTROS_AUDITORIA.items():
                logger.debug(id + ' ' + campos['descricao'])
                self._db['Auditorias'].insert_one(
                    {'id': id,
                     'filtro': campos['filtro'],
                     'order': campos['order'],
                     'descricao': campos['descricao']
                     })
            self.mount_filtros()
            return
        for row in auditorias:
            id = row['id']
            self.dict_auditoria[id] = {
                'filtro': row['filtro'],
                'order': row['order'],
                'descricao': row['descricao']
            }
        logger.debug(self.dict_auditoria)

    def add_relatorio(self, nome: str, relatorio: dict) -> bool:
        """Adiciona um relatório a rodar.

        Recebe um dicionário no formato campos: list e operador:str

        Lendo este dicionário, monta uma checagem por 'metaprogramação'
        """
        self.relatorios[nome] = relatorio

    def reporta(self) -> dict:
        """Executa relatórios configurados.

        Retorna um dicionário com uma lista por chave
        """
        result = defaultdict(list)
        for key, value in self.relatorios:
            cursor = self.db['fs.files'].find(value)
            lista = []
            for linha in cursor:
                lista.append(linha)
            result[key] = lista
        return result
