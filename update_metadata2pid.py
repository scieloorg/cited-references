#!/usr/bin/env python3
import os
import sys

from pymongo import MongoClient
from model.document_manager import DocumentManager as dm
from model.file_manager import FileManager as fm


DEFAULT_DIR = 'data/'


def update_dict(documents: list, metadata2pid: dict, column_indexes: list):
    """
    Receives a list of documents, a metadata2pid dictionary, column indexes (md2pid settings)
    Returns the updated version of the dictionary where each key is a comma separated string composed by the metadata attributes indicated in the m2pid settings.
    """
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
            metadata2pid[metadata_key] = {':'.join([doc[dm.PID], doc.get('collection')])}
        else:
            metadata2pid[metadata_key].add(':'.join([doc[dm.PID], doc.get('collection')]))
    return metadata2pid


if __name__ == "__main__":
    if sys.argv.__len__() == 3:
        LOCAL_DOC_DATABASE_NAME = sys.argv[1]
        NEW_PIDS = sys.argv[2]
    else:
        print('Error: please, provide the name of the local documents database MongoDB (e.g., refSciELO_001)')
        print('Error: please, provide the list of new pids (e.g., new-pids-from-2019-06-10)')
        sys.exit(1)
    
    mongo_client = MongoClient()
    doc_local_database = mongo_client[LOCAL_DOC_DATABASE_NAME]
    list_of_dicts = sorted([DEFAULT_DIR + f for f in os.listdir(DEFAULT_DIR) if f.endswith('.dat')])

    col2newpids = fm.get_col2pids_from_csv(NEW_PIDS)
    print('there are %d new pids' % sum([len(col2newpids.get(col)) for col in col2newpids.keys()]))

    docs = []
    for col in col2newpids.keys():
        print('collecting all document\'s attrs from collection %s' % col)
        new_pids = col2newpids[col]
        c = doc_local_database[col].find({'_id': {'$in': new_pids}})
        tmp_docs = map(dm.get_doc_attrs, c)
        docs.extend(tmp_docs)

    major_keyset = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_TITLE, dm.ISSUE_NUMBER, dm.ISSUE_VOLUME, dm.FIRST_PAGE]
    minor_keyset = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_TITLE, dm.ISSUE_NUMBER, dm.ISSUE_VOLUME]

    print('updating dictionaries')
    new_path_major_dict = DEFAULT_DIR + 'major_dict_new.dat'
    old_major_dict_version = fm.load_dict(DEFAULT_DIR + 'major_dict.dat')
    major_keyset_metadata2pid = update_dict(docs, old_major_dict_version, major_keyset)
    fm.save_dict(major_keyset_metadata2pid, new_path_major_dict)

    new_path_minor_dict = DEFAULT_DIR + 'minor_dict_new.dat'
    old_minor_dict_version = fm.load_dict(DEFAULT_DIR + 'minor_dict.dat')
    minor_keyset_metadata2pid = update_dict(docs, old_minor_dict_version, minor_keyset)
    fm.save_dict(minor_keyset_metadata2pid, new_path_minor_dict)
