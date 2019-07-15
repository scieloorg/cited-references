#!/usr/bin/env python3
import sys

from pymongo import MongoClient


if __name__ == "__main__":
    DATABASE_NAME = sys.argv[1]

    if len(sys.argv) != 2:
        print('Error: enter database name')
        sys.exit(1)

    client = MongoClient()
    database = client[DATABASE_NAME]

    counter = 0
    for i in database.collection_names():
        counter += database[i].count()

    print('there are %d registers in the database %s' % (counter, DATABASE_NAME))
