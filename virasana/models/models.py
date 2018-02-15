from mongoengine import *

class MyFile(Document):
    filename = StringField()
    uploadDate = DateTimeField()
    numeroinformado = StringField()
    dataimportacao = DateTimeField()
    image = FileField()


"""
marmot = Animal.objects(genus='Marmota').first()
photo = marmot.photo.read()
content_type = marmot.photo.content_type
"""