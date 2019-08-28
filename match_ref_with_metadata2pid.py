#!/usr/bin/env python3
import logging
import pickle
import sys

from pymongo import MongoClient
from xylose.scielodocument import Citation
from util.string_processor import StringProcessor


def extract_keys(citation: Citation):
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

    # k1 = ','.join([first_author_first_given_name, first_author_last_surname, publication_date, journal_title, issue_number, '', issue_volume, first_page])
    #
    # k2 = ','.join([first_author_first_given_name, first_author_last_surname, publication_date, journal_title, issue_number, '', issue_volume])
    #
    # k3 = ','.join([first_author_first_given_name_first_char, first_author_last_surname, publication_date, journal_title, issue_number, '', issue_volume, first_page])
    #
    # k4 = ','.join([first_author_first_given_name_first_char, first_author_last_surname, publication_date, journal_title, issue_number, '', issue_volume])
    #
    # k5 = ','.join([first_author_first_given_name, first_author_last_surname, publication_date, journal_title, issue_number, '', issue_volume, first_page])
    #
    # k6 = ','.join([first_author_first_given_name, first_author_last_surname, publication_date, journal_title, issue_number, '', issue_volume])

    # k7 = ','.join([first_author_first_given_name_first_char, first_author_last_surname, publication_date, journal_title, issue_number, issue_volume, first_page])

    # k8 = ','.join([first_author_first_given_name_first_char, first_author_last_surname, publication_date, journal_title, issue_number, issue_volume])

    k7 = ','.join([first_author_first_given_name_first_char, first_author_last_surname, publication_date, journal_title, issue_number, issue_volume, first_page])

    k8 = ','.join([first_author_first_given_name_first_char, first_author_last_surname, publication_date, journal_title, issue_number, issue_volume])

    return k7, k8


def load_dict(path_dict, include_first_page = True):
    tmp_dict = pickle.load(open(path_dict, 'rb'))
    new_dict = {}
    for k, v in tmp_dict.items():
        els = k.split(',')
        first_name_first_char = els[0]
        surname = els[1]
        if len(first_name_first_char) > 0 and len(surname) > 0:
            year = els[2].split('-')[0]
            title = els[3]
            number = els[4]
            order = els[5]
            volume = els[6]
            if include_first_page:
                first_page = els[7]
                new_k = ','.join([first_name_first_char, surname, year, title, number, volume, first_page])
            else:
                new_k = ','.join([first_name_first_char, surname, year, title, number, volume])
            new_v = v.pop()
            if new_k not in new_dict:
                new_dict[new_k] = {new_v}
            else:
                new_dict[new_k].add(new_v)
    return new_dict


if __name__ == "__main__":
    if sys.argv.__len__() == 3:
        LOCAL_DOC_DATABASE_NAME = sys.argv[1]
        DICT_FOLDER = sys.argv[2]
        logging.basicConfig(filename='matches.log', level=logging.INFO, format='%(message)s')
    else:
        print('Error: please, provide the name of the local database MongoDB (e.g., ref_scielo)')
        print('Error: please, provide the path of the dictionaries folder')
        sys.exit(1)

    d7 = load_dict(DICT_FOLDER + 'md2pid_t7_3.dat')
    d8 = load_dict(DICT_FOLDER + 'md2pid_t8_3.dat', include_first_page=False)
    
    mongo_client = MongoClient()

    ref_db = mongo_client[LOCAL_DOC_DATABASE_NAME]

    for col in ref_db.list_collection_names():
        print('matching references from collection %s' % col)
        for reference in ref_db[col].find({}):
            ref_cit = Citation(reference)
            if ref_cit.publication_type in ['article', 'conference']:
                ref_k7, ref_k8 = extract_keys(ref_cit)
                if ref_k7 is not None and ref_k8 is not None:
                    if ref_k7 in d7:
                        logging.info('D7 %s\t KEY %s\tREF %s -> %s' % (col, ref_k7, reference['_id'], d7.get(ref_k7)))
                    if ref_k8 in d8:
                        logging.info('D8 %s\t KEY %s\tREF %s -> %s' % (col, ref_k8, reference['_id'], d8.get(ref_k8)))
