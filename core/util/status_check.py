#!/usr/bin/env python3
import sys

from pymongo import MongoClient


def check_ref_type(json_data):
    """
    This method is the same one that exists in the xylose.scielodocument.Citation
    :param json_data: reference data.
    :return: type of reference according to the presence or not of specific keys.
    """
    if 'v30' in json_data:
        return u'article'
    elif 'v53' in json_data:
        return u'conference'
    elif 'v18' in json_data:
        if 'v51' in json_data:
            return u'thesis'
        else:
            return u'book'
    elif 'v150' in json_data:
        return u'patent'
    elif 'v37' in json_data:
        return u'link'
    else:
        return u'undefined'


if __name__ == "__main__":
    DATABASE_NAME = sys.argv[1]

    if len(sys.argv) != 2:
        print('Error: enter database name')
        sys.exit(1)

    database = MongoClient()[DATABASE_NAME]
    results = {}

    print('collection', 'ref_type', 'with_doi', '+1', '-1', '-2', 'missing status', 'without_doi', 'total', sep='\t')
    for i in database.list_collection_names():
        results[i] = {
            'article': {'with_doi': 0, 'status_1_plus': 0, 'status_1_minus': 0, 'status_2_minus': 0, 'missing': 0,
                        'without_doi': 0},
            'book': {'with_doi': 0, 'status_1_plus': 0, 'status_1_minus': 0, 'status_2_minus': 0, 'missing': 0,
                     'without_doi': 0},
            'conference': {'with_doi': 0, 'status_1_plus': 0, 'status_1_minus': 0, 'status_2_minus': 0, 'missing': 0,
                           'without_doi': 0},
            'link': {'with_doi': 0, 'status_1_plus': 0, 'status_1_minus': 0, 'status_2_minus': 0, 'missing': 0,
                     'without_doi': 0},
            'patent': {'with_doi': 0, 'status_1_plus': 0, 'status_1_minus': 0, 'status_2_minus': 0, 'missing': 0,
                       'without_doi': 0},
            'thesis': {'with_doi': 0, 'status_1_plus': 0, 'status_1_minus': 0, 'status_2_minus': 0, 'missing': 0,
                       'without_doi': 0},
            'undefined': {'with_doi': 0, 'status_1_plus': 0, 'status_1_minus': 0, 'status_2_minus': 0, 'missing': 0,
                          'without_doi': 0}}

        for ref in database[i].find({}):
            ref_type = check_ref_type(ref)
            if 'v237' in ref:
                results[i][ref_type]['with_doi'] += 1
                if 'status' in ref:
                    if ref['status'] == 1:
                        results[i][ref_type]['status_1_plus'] += 1
                    elif ref['status'] == -1:
                        results[i][ref_type]['status_1_minus'] += 1
                    elif ref['status'] == -2:
                        results[i][ref_type]['status_2_minus'] += 1
                else:
                    results[i][ref_type]['missing'] += 1
            else:
                results[i][ref_type]['without_doi'] += 1

        for t in results[i]:
            t_total = sum([results[i][t][x] for x in results[i][t]])
            print(i, t, results[i][t]['with_doi'], results[i][t]['status_1_plus'],
                  results[i][t]['status_1_minus'], results[i][t]['status_2_minus'],
                  results[i][t]['missing'], results[i][t]['without_doi'], t_total, sep='\t')
