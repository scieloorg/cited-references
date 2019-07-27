#!/usr/bin/env python3
import pickle
import sys
import os

from model.document_manager import DocumentManager as dm
from model.file_manager import FileManager as fm
from pymongo import MongoClient


def update_dict(documents: list, metadata2pid: dict, column_indexes: list, use_first_char_author_name: False):
    '''
    Receives a list of documents, a metadata2pid dictionary, column indexes (md2pid settings), and a boolean that indicates the use or not of the first char of the first given name.
    Returns the updated version of the dictionary where each key is a comma separated string composed by the metadata attributes indicated in the m2pid settings.
    '''
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
                    if use_first_char_author_name:
                        column = first_given_name[0]
                    else:
                        column = first_given_name
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
            metadata2pid[metadata_key] = {doc[dm.PID]}
        else:
            metadata2pid[metadata_key].add(doc[dm.PID])
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
    list_of_dicts = sorted([f for f in os.listdir() if f.endswith('.dat')])

    col2newpids = fm.get_col2pids_from_csv(NEW_PIDS)
    print('there are %d new pids' % sum([len(col2newpids.get(col)) for col in col2newpids.keys()]))

    docs = []
    for col in col2newpids.keys():
        print('collecting all document\'s attrs from collection %s' % col)
        new_pids = col2newpids[col]
        c = doc_local_database[col].find({'_id': {'$in': new_pids}})
        tmp_docs = map(dm.get_doc_attrs, c)
        docs.extend(tmp_docs)

    # list of md2pid keys settings
    keyset1 = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_ABBRV, dm.ISSUE_NUMBER, dm.ISSUE_ORDER, dm.ISSUE_VOLUME, dm.FIRST_PAGE]
    keyset2 = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_ABBRV, dm.ISSUE_NUMBER, dm.ISSUE_ORDER, dm.ISSUE_VOLUME]
    keyset3 = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_ABBRV, dm.ISSUE_NUMBER, dm.ISSUE_ORDER, dm.ISSUE_VOLUME, dm.FIRST_PAGE]
    keyset4 = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_ABBRV, dm.ISSUE_NUMBER, dm.ISSUE_ORDER, dm.ISSUE_VOLUME]
    keyset5 = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_TITLE, dm.ISSUE_NUMBER, dm.ISSUE_ORDER, dm.ISSUE_VOLUME, dm.FIRST_PAGE]
    keyset6 = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_TITLE, dm.ISSUE_NUMBER, dm.ISSUE_ORDER, dm.ISSUE_VOLUME]
    keyset7 = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_TITLE, dm.ISSUE_NUMBER, dm.ISSUE_ORDER, dm.ISSUE_VOLUME, dm.FIRST_PAGE]
    keyset8 = [dm.FIRST_AUTHOR_GIVEN_NAMES, dm.FIRST_AUTHOR_SURNAME, dm.PUBLICATION_DATE, dm.JOURNAL_TITLE, dm.ISSUE_NUMBER, dm.ISSUE_ORDER, dm.ISSUE_VOLUME]

    print('updating dictionaries')
    for i, keyset in enumerate([keyset1, keyset2, keyset3, keyset4, keyset5, keyset6, keyset7, keyset8]):
        new_version_number = str(int(list_of_dicts[i].split('_')[-1].split('.')[0]) + 1)
        path_dict_name_new_version = 'md2pid_t' + str(i + 1) + '_' + new_version_number + '.dat'
        if i + 1 == 3 or i + 1 == 4 or i + 1 == 7 or i + 1 == 8:
            use_first_char_author_name = True
        else:
            use_first_char_author_name = False
        old_dict_version = fm.load_dict(list_of_dicts[i])
        keyset_metadata2pid = update_dict(docs, old_dict_version, keyset, use_first_char_author_name)
        fm.save_dict(keyset_metadata2pid, path_dict_name_new_version)
