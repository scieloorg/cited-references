#!/usr/bin/env python3
import sys

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


if __name__ == "__main__":
    if sys.argv.__len__() == 2:
        LOCAL_DOC_DATABASE_NAME = sys.argv[1]
    else:
        print('Error: please, provide the name of the local database MongoDB (e.g., refSciELO_001')
        sys.exit(1)
    
    mongo_client = MongoClient()
    doc_local_database = mongo_client[LOCAL_DOC_DATABASE_NAME]
    ref_local_database = mongo_client['ref_scielo']

    for col in doc_local_database.collection_names():
        db_doc_col = doc_local_database[col]
        print('creating collection into the references database for %s' % col)
        for doc in db_doc_col.find():
            citations = doc.get('citations')
            for ci in citations:
                ref_id = ci.get('v701')[0].get('_')
                ref_key = '_'.join([doc.get('code'), ref_id])
                ref_value = ci
                json_ref = ci
                json_ref['_id'] = ref_key
                try:
                    ref_local_database[col].insert_one(json_ref)
                except DuplicateKeyError:
                    print('%s is already in the database' % ref_key)
