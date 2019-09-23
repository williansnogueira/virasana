from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, TextAreaField
from wtforms.validators import data_required


class SelectAuditoria(FlaskForm):
    filtro_auditoria = SelectField(u'Filtros de Auditoria',
                                   default=0)


class FormAuditoria(FlaskForm):
    """Tela de cadastro de Filtro de Auditoria

    """
    id = StringField(u'ID', validators=[data_required()], default='')
    descricao = TextAreaField(u'Descrição',
                              render_kw={"rows": 2, "cols": 200},
                              validators=[data_required()], default='')
    filtro = TextAreaField(u'Filtro',
                              render_kw={"rows": 6, "cols": 200},
                              validators=[data_required()], default='{}')
    order = TextAreaField(u'Ordem',
                          render_kw={"rows": 1, "cols": 200},
                          validators=[data_required()], default='')
