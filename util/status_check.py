#!/usr/bin/env python3
from pymongo import MongoClient

import sys


if __name__ == "__main__":
    DATABASE_NAME = sys.argv[1]
    EXECUTION_MODE = sys.argv[2]
    
    if len(sys.argv) != 3:
        print('Error: enter database name')
        print('Error: enter execution mode [article, default]')
        sys.exit(1)

    client = MongoClient()
    database = client[DATABASE_NAME]

    print('col\ttotal\tdoi\t+1\t-1\t-2\tmissing')

    if EXECUTION_MODE == 'default':
        for i in database.list_collection_names():
            total_docs = database[i].count_documents({})
            with_doi = database[i].count_documents({'v237': {'$exists': True}})
            status_1_minus = database[i].count_documents({'status': -1})
            status_1_plus = database[i].count_documents({'status': 1})
            status_2_minus = database[i].count_documents({'status': -2})
            missing = with_doi - sum([status_1_minus, status_1_plus])
            print(i, total_docs, with_doi, status_1_plus, status_1_minus, status_2_minus, missing, sep='\t')
    elif EXECUTION_MODE == 'article':
        for i in database.list_collection_names():
            total_docs = database[i].count({'v709': [ {'_': 'article'} ]})
            with_doi = database[i].count({'$and': [{'v237': {'$exists': True}}, {'v709': [ {'_': 'article'} ]}]})
            status_1_minus = database[i].count({'$and': [{'status': -1}, {'v709': [ {'_': 'article'} ]}]})
            status_1_plus = database[i].count({'$and': [{'status': 1}, {'v709': [ {'_': 'article'} ]}]})
            status_2_minus = database[i].count({'$and': [{'status': -2}, {'v709': [ {'_': 'article'} ]}]})          
            missing = with_doi - sum([status_1_minus, status_1_plus])
            print(i, total_docs, with_doi, status_1_plus, status_1_minus, status_2_minus, missing, sep='\t')
