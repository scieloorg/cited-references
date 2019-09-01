#!/usr/bin/env python3
import os
import sys

from pymongo import MongoClient
from model.document_manager import DocumentManager as dm
from model.file_manager import FileManager as fm


DEFAULT_DIR = 'data/'


def create_dict(documents: list, column_indexes: list):
    """
    Receives a list of documents, column indexes (md2pid settings).
    Returns a dictionary where each key is a comma separated string composed by the metadata attributes indicated in the m2pid settings.
    """
    metadata2pid = {}
    for doc in documents:
        doc_attrs = []
        for i in column_indexes:
            if i == dm.FIRST_AUTHOR_GIVEN_NAMES:
                given_names = doc[dm.FIRST_AUTHOR_GIVEN_NAMES].split(' ')
                if len(given_names) > 0:
                    first_given_name = given_names[0]
                else:
                    first_given_name = ''
                if len(first_given_name) > 0:
                    column = first_given_name[0]
                else:
                    column = ''
            elif i == dm.FIRST_AUTHOR_SURNAME:
                surnames = doc[dm.FIRST_AUTHOR_SURNAME].split(' ')
                if len(surnames) > 1:
                    column = surnames[-1]
                else:
                    column = surnames[-1]
            else:
                column = doc[i]
            doc_attrs.append(column)
        metadata_key = ','.join(doc_attrs)
        if metadata_key not in metadata2pid:
            metadata2pid[metadata_key] = {':'.join([doc[dm.PID], doc[-1]])}
        else:
            metadata2pid[metadata_key].add(':'.join([doc[dm.PID], doc[-1]]))
    return metadata2pid


if __name__ == "__main__":
    if sys.argv.__len__() == 2:
        LOCAL_DOC_DATABASE_NAME = sys.argv[1]
    else:
        print('Error: please, provide the name of the local database MongoDB (e.g., refSciELO_001)')
        sys.exit(1)

    os.makedirs(DEFAULT_DIR)

    mongo_client = MongoClient()
    doc_local_database = mongo_client[LOCAL_DOC_DATABASE_NAME]
    docs = []
    for col in doc_local_database.list_collection_names():
        print('collecting attributes from collection %s' % col)
        c = doc_local_database[col].find({})
        tmp_docs = map(dm.get_doc_attrs, c)
        docs.extend(tmp_docs)

    # use or not first page info
    major_keyset = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_TITLE, dm.ISSUE_NUMBER, dm.ISSUE_VOLUME, dm.FIRST_PAGE]
    minor_keyset = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_TITLE, dm.ISSUE_NUMBER, dm.ISSUE_VOLUME]

    print('creating dictionaries')

    major_keyset_metadata = create_dict(docs, major_keyset)
    print('there are %d keys in the major_keyset_metadata' % len(major_keyset_metadata))
    fm.save_dict(major_keyset_metadata,  DEFAULT_DIR + 'major_dict.dat')

    minor_keyset_metadata = create_dict(docs, minor_keyset)
    print('there are %d keys in the minor_keyset_metadata' % len(minor_keyset_metadata))
    fm.save_dict(minor_keyset_metadata,  DEFAULT_DIR + 'minor_dict.dat')
