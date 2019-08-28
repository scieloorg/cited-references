#!/usr/bin/env python3
import sys

from pymongo import MongoClient
from model.document_manager import DocumentManager as dm
from model.file_manager import FileManager as fm


def create_dict(documents: list, column_indexes: list, use_first_char_author_name: False):
    '''
    Receives a list of documents, column indexes (md2pid settings), and a boolean that indicates the use or not of the first char of the first given name.
    Returns a dictionary where each key is a comma separated string composed by the metadata attributes indicated in the m2pid settings.
    '''
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
    if sys.argv.__len__() == 2:
        LOCAL_DOC_DATABASE_NAME = sys.argv[1]
    else:
        print('Error: please, provide the name of the local database MongoDB (e.g., refSciELO_001)')
        sys.exit(1)
    
    mongo_client = MongoClient()
    doc_local_database = mongo_client[LOCAL_DOC_DATABASE_NAME]
    docs = []
    for col in doc_local_database.list_collection_names():
        print('collecting attributes from collection %s' % col)
        c = doc_local_database[col].find({})
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

    print('creating dictionaries')
    for i, keyset in enumerate([keyset1, keyset2, keyset3, keyset4, keyset5, keyset6, keyset7, keyset8]):
        path_dict_name = 'md2pid_t' + str(i + 1) + '_1' + '.dat'
        if i + 1 == 3 or i + 1 == 4 or i + 1 == 7 or i + 1 == 8:
            use_first_char_author_name = True
        else:
            use_first_char_author_name = False
        keyset_metadata2pid = create_dict(docs, keyset, use_first_char_author_name)
        print('there are %d keys in the metadata2pid with keyset%d' % (len(keyset_metadata2pid), i + 1))
        fm.save_dict(keyset_metadata2pid, path_dict_name)
