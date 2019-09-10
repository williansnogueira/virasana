
import json
from bson import ObjectId


def update_due(db, dues):
    # print(dues)
    for _id, due in dues.items():
        print('Updating %s ' % _id)
        print('with %s ' % json.dumps(due)[:50])
        result = db.fs.files.update_one(
            {'_id': ObjectId(_id)},
            {'$set': {'metadata.due': due}}
        )
        print(result)
