


def update_due(db, dues):
    for _id, due in dues.items():
        db.fs.files.update_one(
            {'_id': _id},
            {'$set': {'metadata.due': due}}
        )
