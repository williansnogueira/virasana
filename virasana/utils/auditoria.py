"""Funções para Auditoria/comparação nos metadados de fs.files."""
from collections import defaultdict


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
          }
}


class Auditoria():
    """Recebe params, monta consultas de auditoria entre campos fs.files."""

    def __init__(self, db):
        """Init.

        Args:
            db: conexão ao MongoDB a ser usada
        """
        self.db = db
        self.relatorios = {}

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
