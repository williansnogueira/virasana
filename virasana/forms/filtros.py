from datetime import datetime
from flask import g
from flask_login import current_user
from flask_wtf import FlaskForm
from wtforms import BooleanField, DateField, IntegerField, FloatField, \
    SelectField, StringField
from wtforms.validators import optional

from ajna_commons.utils.sanitiza import mongo_sanitizar
from virasana.models.auditoria import Auditoria

MAXROWS = 50
MAXPAGES = 100


class FormFiltro(FlaskForm):
    """Valida pesquisa de arquivos.

    Usa wtforms para facilitar a validação dos campos de pesquisa da tela
    search_files.html

    """
    numero = StringField(u'Número', validators=[optional()], default='')
    start = DateField('Start', validators=[optional()])
    end = DateField('End', validators=[optional()])
    alerta = BooleanField('Alerta', validators=[optional()], default=False)
    zscore = FloatField('Z-Score', validators=[optional()], default=3.)
    order = None
    pagina_atual = None

    def recupera_filtro_personalizado(self):
        """Usa variável global para guardar filtros personalizados entre posts."""
        key = 'filtros' + current_user.id
        self.filtro_personalizado = g.get(key)

    def valida(self, filtro, db):
        """Lê formulário e adiciona campos ao filtro se necessário."""
        if self.validate():  # configura filtro básico
            self.filtro = {}
            pagina_atual = self.pagina_atual.data
            numero = self.numero.data
            start = self.start.data
            end = self.end.data
            alerta = self.alerta.data
            if numero == 'None':
                numero = None
            if start and end:
                start = datetime.combine(start, datetime.min.time())
                end = datetime.combine(end, datetime.max.time())
                self.filtro['metadata.dataescaneamento'] = {'$lte': end, '$gte': start}
            if numero:
                self.filtro['metadata.numeroinformado'] = \
                    {'$regex': '^' + mongo_sanitizar(self.numero), '$options': 'i'}
            if alerta:
                self.filtro['metadata.xml.alerta'] = True
