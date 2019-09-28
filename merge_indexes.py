import logging
import os
import sys


BASE2COLUMN_INDEXES = {
        'doaj': {
            'issn': [3, 4],
            'title': [0, 2]
        },
        'latindex': {
            'issn-l': [8],
            'issn': [7],
            'title': [14, 22, 23, 24, 25, 26]
        },
        'portal_issn': {
            'issn': [0, 5, 6, 13],
            'title': [2, 3, 4, 7, 8, 9, 10, 11, 12]
        },
        'scielo': {
            'issn': [1, 2, 3],
            'title': [4, 5, 6, 8, 10, 11]
        },
        'scimago_jr': {
            'issn': [4],
            'title': [2]
        },
        'scopus': {
            'issn': [19, 20],
            'title': [1]
        }, 
        'ulrich': {
            'issn': [4],
            'title': [5, 6]
        },
        'wos': {
            'issn': [5, 6],
            'title': [2]
        },
        'wos_jcr': {
            'issn': [3],
            'title': [1, 2]
        }
}


def read_base(path_base: str, base_name: str, sep='\t', ignore_first_line=False):
    dict_base = {'issn-l': [], 'issn': [], 'title': []}

    try:
        file_base = open(path_base)
        line = file_base.readline()

        if ignore_first_line:
            line = file_base.readline()

        issn_l_index = BASE2COLUMN_INDEXES.get(base_name).get('issn-l')
        issn_indexes = BASE2COLUMN_INDEXES.get(base_name).get('issn')
        title_indexes = BASE2COLUMN_INDEXES.get(base_name).get('title')
 
        while line:
            els = line.split(sep)
            if issn_l_index == None:
                pass
            else:
                issn_l = els[issn_l_index]

            issns = [els[i] for i in issn_indexes]

            # TODO: it is necessary to clean/normalize the journal's titles
            #       use the preprocessing script already implemented
            titles = [els[t] for t in title_indexes]

            if issn_l not in dict_base:
                dict_base[issn_l] = [[issn], [title]]
            else:
                # TODO: it is necessary to unify each list contents
                dict_base[issn_l][0].append(issn)
                dict_base[issn_l][1].append(title)

            line = file_base.readline()
        
        return dict_base

    except FileNotFoundError:
        logging.warning('FILE_NOT_FOUND %s' % path_base)


def merge_bases(bases: list):
    pass


DEFAULT_DIR_INDEXES = 'bases/'

if __name__ == '__main__':
    logging.basicConfig(filename='merge_indexes.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if len(sys.argv) == 2:
        DEFAULT_DIR_INDEXES = sys.argv[1]

    path_bases = [b for b in os.listdir(DEFAULT_DIR_INDEXES)]
    bases = []
    for pb in path_bases:
        bases.extend(read_base(pb))
    merged_bases = merge_bases(bases)
    save_bases(merged_bases)

