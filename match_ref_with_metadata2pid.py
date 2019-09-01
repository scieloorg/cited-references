#!/usr/bin/env python3
import logging
import pickle
import sys

from pymongo import MongoClient
from xylose.scielodocument import Citation
from util.string_processor import StringProcessor


def extract_keys(citation: Citation):
    """
    Extract a key from a set of citation's fields
    :param citation: a object of the class Citation
    :return: tuple of keys (represented by a comma separated string) where the first key has the first page
    """
    if citation.first_author:
        first_author_first_given_name = StringProcessor.preprocess_name(citation.first_author.get('given_names', '').replace('.',' ').split(' ')[0]).lower()
        if len(first_author_first_given_name) > 0:
            first_author_first_given_name_first_char = first_author_first_given_name[0].lower()
        else:
            first_author_first_given_name_first_char = ''
        first_author_last_surname = StringProcessor.preprocess_name(citation.first_author.get('surname', '').replace('.', ' ').replace(';', ' ').split(' ')[-1]).lower()
        if first_author_last_surname == '':
            return None, None
    else:
        return None, None

    publication_date = citation.publication_date
    if not publication_date:
        publication_date = ''

    if citation.source:
        journal_title = StringProcessor.preprocess_journal_title(citation.source).lower()
    else:
        journal_title = ''

    issue_number = citation.issue
    if not issue_number:
        issue_number = ''

    issue_volume = citation.volume
    if not issue_volume:
        issue_volume = ''

    first_page = citation.first_page
    if not first_page:
        first_page = ''

    major_key = ','.join([first_author_first_given_name_first_char, first_author_last_surname, publication_date, journal_title, issue_number, issue_volume, first_page])
    minor_key = ','.join([first_author_first_given_name_first_char, first_author_last_surname, publication_date, journal_title, issue_number, issue_volume])

    return major_key, minor_key


if __name__ == "__main__":
    if sys.argv.__len__() == 3:
        LOCAL_DOC_DATABASE_NAME = sys.argv[1]
        DICT_FOLDER = sys.argv[2]
        logging.basicConfig(filename='matches.log', level=logging.INFO, format='%(message)s')
    else:
        print('Error: please, provide the name of the local database MongoDB (e.g., ref_scielo)')
        print('Error: please, provide the path of the dictionaries folder')
        sys.exit(1)

    major_dict = pickle.load(open(DICT_FOLDER + 'major_dict.dat', 'rb'))
    minor_dict = pickle.load(open(DICT_FOLDER + 'minor_dict.dat', 'rb'))
    
    mongo_client = MongoClient()

    ref_db = mongo_client[LOCAL_DOC_DATABASE_NAME]

    for col in ref_db.list_collection_names():
        print('matching references from collection %s' % col)
        for reference in ref_db[col].find({}):
            ref_cit = Citation(reference)
            if ref_cit.publication_type in ['article', 'conference']:
                major_key, minor_key = extract_keys(ref_cit)
                if major_key is not None and minor_key is not None:
                    if major_key in major_dict:
                        logging.info('MAJOR %s\t KEY %s\tREF %s -> %s' % (col, major_key, reference['_id'], major_dict.get(major_key)))
                    if minor_key in minor_dict:
                        logging.info('MINOR %s\t KEY %s\tREF %s -> %s' % (col, minor_key, reference['_id'], minor_dict.get(minor_key)))
