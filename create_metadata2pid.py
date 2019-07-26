#!/usr/bin/env python3
import pickle
import sys

from pymongo import MongoClient
from utils.string_processor import StringProcessor
from xylose.scielodocument import Article, UnavailableMetadataException


COLLECTION = 0
PID = 1
DOCUMENT_TYPE = 2
FIRST_AUTHOR_GIVEN_NAMES = 3
FIRST_AUTHOR_SURNAME = 4
PUBLICATION_DATE = 5
JOURNAL_TITLE = 6
JOURNAL_ABBRV = 7
JOURNAL_ISSN_PPUB = 8
JOURNAL_ISSN_EPUB = 9
ISSUE_NUMBER = 10
ISSUE_ORDER = 11
ISSUE_VOLUME = 12
FIRST_PAGE = 13


def get_documents_attributes(doc_db):
    documents = []
    for col in doc_db.list_collection_names():
        print('collecting document attributes from %s' % col)
        docs_cursor = doc_db[col].find({})
        for doc in docs_cursor:
            pid = doc.get('_id')
            xydoc = Article(doc)

            document_type = xydoc.document_type.lower()
            first_author = xydoc.first_author

            if first_author == None:
                first_author = {}

            if 'given_names' in first_author:
                first_author_given_names = StringProcessor.preprocess_name(first_author.get('given_names', '').lower())
            else:
                first_author_given_names = ''
                
            if 'surname' in first_author:
                first_author_surname = StringProcessor.preprocess_name(first_author.get('surname', '').lower())
            else:
                first_author_surname = ''

            publication_date = xydoc.publication_date
            journal_title = StringProcessor.preprocess_journal_title(xydoc.journal.title.lower())
            journal_abbrev_title = StringProcessor.preprocess_journal_title(xydoc.journal.abbreviated_title.lower())

            journal_issn_ppub = xydoc.journal.print_issn
            if journal_issn_ppub is None:
                journal_issn_ppub = ''
                
            journal_issn_epub = xydoc.journal.electronic_issn
            if journal_issn_epub is None:
                journal_issn_epub = ''

            try:
                issue_number = xydoc.issue.number          
                issue_order = xydoc.issue.order
                issue_volume = xydoc.issue.volume
            except UnavailableMetadataException:
                pass
            except ValueError:
                pass

            if issue_number is None:
                issue_number = ''
            
            if issue_order is None:
                issue_order = ''

            if issue_volume is None:
                issue_volume = ''

            start_page = xydoc.start_page
            if xydoc.start_page is None:
                start_page = ''

            documents.append([col, pid, document_type, first_author_given_names, first_author_surname, publication_date, journal_title, journal_abbrev_title, journal_issn_ppub, journal_issn_epub, issue_number, issue_order, issue_volume, start_page])

            del xydoc
        del docs_cursor
    return documents


def create_dict(docs: list, column_indexes: list, use_first_char_author_name: False):
    metadata2pid = {}
    for d in docs:
        mdoc = []
        for i in column_indexes:
            if i == FIRST_AUTHOR_GIVEN_NAMES:
                given_names = d[FIRST_AUTHOR_GIVEN_NAMES].split(' ')
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
            elif i == FIRST_AUTHOR_SURNAME:
                surnames = d[FIRST_AUTHOR_SURNAME].split(' ')
                if len(surnames) > 1:
                    column = surnames[-1]
                else:
                    column = surnames[-1]
            else:
                column = d[i]
            mdoc.append(column)
        metadata_key = ','.join(mdoc)
        if metadata_key not in metadata2pid:
            metadata2pid[metadata_key] = {d[PID]}
        else:
            metadata2pid[metadata_key].add(d[PID])
    return metadata2pid


def save_dict(docs: dict, path_file_dict: str):
    file_dict = open(path_file_dict, 'wb')
    pickle.dump(docs, file_dict)


if __name__ == "__main__":
    if sys.argv.__len__() == 2:
        LOCAL_DOC_DATABASE_NAME = sys.argv[1]
    else:
        print('Error: please, provide the name of the local database MongoDB (e.g., refSciELO_001')
        sys.exit(1)
    
    mongo_client = MongoClient()
    doc_local_database = mongo_client[LOCAL_DOC_DATABASE_NAME]

    docs = get_documents_attributes(doc_local_database)

    dict_1 = [FIRST_AUTHOR_GIVEN_NAMES, FIRST_AUTHOR_SURNAME, PUBLICATION_DATE, JOURNAL_ABBRV, ISSUE_NUMBER, ISSUE_ORDER, ISSUE_VOLUME, FIRST_PAGE]
    dict_2 = [FIRST_AUTHOR_GIVEN_NAMES, FIRST_AUTHOR_SURNAME, PUBLICATION_DATE, JOURNAL_ABBRV, ISSUE_NUMBER, ISSUE_ORDER, ISSUE_VOLUME]
    dict_3 = [FIRST_AUTHOR_GIVEN_NAMES, FIRST_AUTHOR_SURNAME, PUBLICATION_DATE, JOURNAL_ABBRV, ISSUE_NUMBER, ISSUE_ORDER, ISSUE_VOLUME, FIRST_PAGE]
    dict_4 = [FIRST_AUTHOR_GIVEN_NAMES, FIRST_AUTHOR_SURNAME, PUBLICATION_DATE, JOURNAL_ABBRV, ISSUE_NUMBER, ISSUE_ORDER, ISSUE_VOLUME]
    dict_5 = [FIRST_AUTHOR_GIVEN_NAMES, FIRST_AUTHOR_SURNAME, PUBLICATION_DATE, JOURNAL_TITLE, ISSUE_NUMBER, ISSUE_ORDER, ISSUE_VOLUME, FIRST_PAGE]
    dict_6 = [FIRST_AUTHOR_GIVEN_NAMES, FIRST_AUTHOR_SURNAME, PUBLICATION_DATE, JOURNAL_TITLE, ISSUE_NUMBER, ISSUE_ORDER, ISSUE_VOLUME]
    dict_7 = [FIRST_AUTHOR_GIVEN_NAMES, FIRST_AUTHOR_SURNAME, PUBLICATION_DATE, JOURNAL_TITLE, ISSUE_NUMBER, ISSUE_ORDER, ISSUE_VOLUME, FIRST_PAGE]
    dict_8 = [FIRST_AUTHOR_GIVEN_NAMES, FIRST_AUTHOR_SURNAME, PUBLICATION_DATE, JOURNAL_TITLE, ISSUE_NUMBER, ISSUE_ORDER, ISSUE_VOLUME]

    print('creating dictionaries')
    for i, d in enumerate([dict_1, dict_2, dict_3, dict_4, dict_5, dict_6, dict_7, dict_8]):
        path_test_name = 'd' + str(i + 1) + '.dat'
        if i + 1 == 3 or i + 1 == 4 or i + 1 == 7 or i + 1 == 8:
            use_first_char_author_name = True
        else:
            use_first_char_author_name = False
        mdocs = create_dict(docs, d, use_first_char_author_name)
        print('d', str(i+1), str(len(mdocs)))
        save_dict(mdocs, path_test_name)
