from marshmallow_sqlalchemy import ModelSchema

from virasana.integracao.mercante.mercantealchemy import Manifesto, ConteinerVazio, \
    Conhecimento, Item, NCMItem


class BaseSchema(ModelSchema):
    excluded_keys = ['create_date', 'last_modified',
                     'dataInicioOperacaoDate']

    def dump(self, objeto):
        original = super().dump(objeto)
        result = {}
        for k, v in self.carga_fields.items():
            result[k] = original.get(v)
        return result


class ManifestoSchema(BaseSchema):
    carga_fields = \
        {'manifesto': 'numero',
         'codigoportocarregamento': 'portoCarregamento',
         'codigoportodescarregamento': 'portoDescarregamento',
         'codigoterminalcarregamento': 'codigoTerminalCarregamento',
         'codigoempresanavegacao': 'codigoEmpresaNavegacao',
         'codigoterminaldescarregamento': 'codigoTerminalDescarregamento',
         'codigoagencianavegacao': 'codAgenciaInformante'}

    class Meta:
        model = Manifesto

    def dump(self, manifesto):
        result = super().dump(manifesto)
        result['tipomanifesto'] = Enumerado.getTipoManifesto(session, manifesto.tipoTrafego)
        return result


class ConteinerVazioSchema(BaseSchema):
    carga_fields = \
        {'container': 'idConteinerVazio',
         'tara(kg)': 'taraConteinerVazio'}

    class Meta:
        model = ConteinerVazio


class ItemSchema(BaseSchema):
    carga_fields = \
        {'container': 'codigoConteiner',
         'item': 'numeroSequencialItemCarga',
         'lacre': 'lacre',
         'taracontainer': 'tara',
         'volumeitem': 'cubagemM3',
         'pesobrutoitem': 'pesoBruto',
         'indicadorusoparcial': 'indicadorUsoParcial',
         # 'tipoItemCarga':
         }

    class Meta:
        model = Item


class NCMItemSchema(BaseSchema):
    carga_fields = \
        {'ncm': 'identificacaoNCM',
         'item': 'numeroSequencialItemCarga',
         'descricao': 'descritivo',
         'embalagem': 'codigoTipoEmbalagem',
         'madeira': 'itemEmbaladoMadeira',
         'marca': 'marcaMercadoria',
         'numeroidentificacao': 'numeroIdentificacao',
         'qtdevolumes': 'qtdeVolumes'}

    class Meta:
        model = NCMItem


class ConhecimentoSchema(BaseSchema):
    carga_fields = \
        {'conhecimento': 'numeroCEmercante',
         'codigoagentenavegacao': 'codigoEmpresaNavegacao',
         'cpfcnpjconsignatario': 'consignatario',
         'cubagem': 'cubagem',
         'dataemissao': 'dataEmissao',
         'descricaomercadoria': 'descricao',
         'identificacaoembarcador': 'embarcador',
         'manifesto': 'manifestoCE',
         'paisdestino': 'paisDestinoFinalMercante',
         'codigoportodestino': 'portoDestFinal',
         'codigoportoorigem': 'portoOrigemCarga',
         'tipo': 'tipoBLConhecimento'}

    class Meta:
        model = Conhecimento


manifesto_schema = ManifestoSchema()
conteinervazio_schema = ConteinerVazioSchema()
item_schema = ItemSchema()
ncmitem_schema = NCMItemSchema()
conhecimento_schema = ConhecimentoSchema()


def manifesto_carga(session, numero):
    dict_carga = {'vazio': True}
    manifesto = session.query(Manifesto).filter(Manifesto.numero == numero).one()
    dict_carga['manifesto'] = manifesto_schema.dump(manifesto)
    conteineres = session.query(ConteinerVazio).filter(ConteinerVazio.manifesto == numero).all()
    dict_carga['conteineres'] = [conteinervazio_schema.dump(conteiner)
                                 for conteiner in conteineres]
    return dict_carga



def conhecimento_carga(session, numero):
    dict_carga = {'vazio': False}
    conhecimento = session.query(Conhecimento).filter(Conhecimento.numeroCEmercante == numero).one()
    dict_carga['manifesto'] = conhecimento_schema.dump(conhecimento)
    itens = session.query(Item).filter(Item.numeroCEmercante == numero).all()
    dict_carga['itens'] = [item_schema.dump(item)
                                 for item in itens]
    return dict_carga