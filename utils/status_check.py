#!/usr/bin/env python3
from pymongo import MongoClient

import sys


if __name__ == "__main__":
    DATABASE_NAME = sys.argv[1]
    
    if len(sys.argv) != 2:
        print('Error: enter database name')
        sys.exit(1)

    client = MongoClient()
    database = client[DATABASE_NAME]

    print('col\ttotal\tdoi\t+1\t-1\t-2\tfetch')
    for i in database.list_collection_names():
        total_docs = database[i].count_documents({})
        with_doi = database[i].count_documents({'v237': {'$exists': True}})
        status_1_minus = database[i].count_documents({'status': -1})
        status_1_plus = database[i].count_documents({'status': 1})
        status_2_minus = database[i].count_documents({'status': -2})
        to_fetch = with_doi - sum([status_1_minus, status_1_plus, status_2_minus])
        print(i, total_docs, with_doi, status_1_plus, status_1_minus, status_2_minus, to_fetch, sep='\t')
