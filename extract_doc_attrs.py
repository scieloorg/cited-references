#!/usr/bin/env python3
import sys

from datetime import datetime
from model.document_manager import DocumentManager as dm
from pymongo import MongoClient


def get_and_save_doc_attrs(doc_db, path_csv, processing_date:datetime):
    """
    Receive a document's database, the path of a csv file to be writed and a datetime object.
    Writes the documents attributes in the csv file, according to the processing date
    """
    file_doc_attrs = open(path_csv, 'w')
    for col in doc_db.list_collection_names():
        print('collecting document attributes from %s' % col)
        docs_cursor = doc_db[col].find({
            "$expr": {
                "$gte": [
                    {
                    "$dateFromString": { 
                        "dateString": "$processing_date", "format": "%Y-%m-%d" 
                        }
                    },
                    processing_date
                ]
            }
        })
        for doc in docs_cursor:
            doc_attrs = dm.get_doc_attrs(doc)
            file_doc_attrs.write(','.join(doc_attrs) + '\n')

        del docs_cursor
    file_doc_attrs.close()


if __name__ == "__main__":
    if sys.argv.__len__() == 3:
        LOCAL_DOC_DATABASE_NAME = sys.argv[1]
        FROM_DATE = sys.argv[2]
    else:       
        print('Error: please, provide the name of the local database MongoDB (e.g., refSciELO_001)')
        print('Error: please, provide the date of collecting (e.g., 2019-5-20)')
        sys.exit(1)
    
    PATH_CSV = 'new-doc-attrs-from-' + FROM_DATE + '.csv'
    PROCESSING_DATE = datetime(*[int(i) for i in FROM_DATE.split('-')])

    mongo_client = MongoClient()
    doc_local_database = mongo_client[LOCAL_DOC_DATABASE_NAME]

    get_and_save_doc_attrs(doc_local_database, PATH_CSV, PROCESSING_DATE)
