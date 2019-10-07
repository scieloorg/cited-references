import logging
import sys

from util.string_processor import StringProcessor


DEFAULT_DIR_INDEXES = 'bases/'
DEFAULT_MODE = 'create_base'

BASE2COLUMN_INDEXES = {
        'doaj': {
            'issn': [2, 3],
            'title': [0, 1],
            'sep': '\t'
        },
        'latindex': {
            'issn': [0],
            'title': [2, 3, 4],
            'sep': '\t'
        },
        'portal_issn': {
            'issn_l': 0,
            'issn': [1],
            'title': [2, 3, 4, 5],
            'sep': '\t'
        },
        'scielo': {
            'issn': [0, 1, 2],
            'title': [3, 4, 5],
            'sep': '\t'
        },
        'scimago_jr': {
            'issn': [1, 2],
            'title': [0],
            'sep': '\t'
        },
        'scopus': {
            'issn': [1, 2],
            'title': [0],
            'sep': '\t'
        }, 
        'ulrich': {
            'issn': [0],
            'title': [1],
            'sep': '\t'
        },
        'wos': {
            'issn': [1, 2],
            'title': [0],
            'sep': '\t'
        },
        'wos_jcr': {
            'issn': [2],
            'title': [0, 1],
            'sep': '\t'
        }
}


def is_valid_issn(issn: str):
    """
    Checks if an issn is valid
    :param issn: the original issn code
    :return: True if the issn is valid, False otherwise
    """
    issn = issn.replace('-', '')
    if len(issn) == 8:
        return True
    return False


def is_valid_title(title: str):
    """
    Checks if a title is valid (has at least one char)
    :param title: the original title
    :return: True if the title is valid, False otherwise
    """
    if len(title) > 0:
        return True
    return False


def get_issnl_from_dict(issns: list, issn2issnl: dict):
    ls = list(set([issn2issnl.get(i, '') for i in issns if issn2issnl.get(i, '') != '']))
    if len(ls) == 1:
        return ls[0]
    elif len(ls) == 0:
        logging.warning('%s is not in the list' % issns)
        return None
    else:
        logging.warning('%s links to multiple issn-l' % issns)
        return None


def get_issnl_from_base(issns: list, col_index: int, issn2issnl: dict):
    i = issns[col_index]
    if i in issn2issnl:
        return i
    else:
        logging.warning('%s is not in dict' % i)
        return None


def has_valid_issn(issns: list):
    """
    Checks if a list of issns have at least one valid issn
    :param issns: a list of issns
    :return: True if the list contains at least one valid issn, False otherwise
    """
    for i in issns:
        if is_valid_issn(i):
            return True
    return False


def has_valid_title(titles: list):
    """
    Checks if a list of titles have at least one valid title
    :param titles: a list of titles
    :return: True if the list contains at least one valid title, False otherwise
    """
    for t in titles:
        if is_valid_title(t):
            return True
    return False


def check_char_freq(titles: list):
    """
    Counts the number of ocurrences of each char present in all titles
    :param titles: a list of all the titles
    :return: a dict where each key is a char and each value is its number of ocurrences
    """
    dict_char = {}
    for t in titles:
        for c in t:
            if c.lower() not in dict_char:
                dict_char[c.lower()] = 1
            else:
                dict_char[c.lower()] += 1
    return dict_char


def save_char_freq(c2freq: dict):
    """
    Saves the char2freq dictionary into the disk
    :param c2freq: a dictionary where each key is a char and each value is composed by the preprocessed version of the char and the char's number of ocurrences
    """
    final_c2freq = open(DEFAULT_DIR_INDEXES + '../char_freq.csv', 'w')
    for k in sorted(c2freq, key=lambda x: c2freq.get(x), reverse=True):
        final_c2freq.write('%s\t%s\t%d' % (k, StringProcessor.preprocess_journal_title(k), c2freq.get(k)) + '\n')
    final_c2freq.close()


def mount_issn2issnl_dict(path_file_portal_issn: str):
    """
    Mounts a dictionary where each key is a ISSN and each value its ISSN-L
    :param path_file_portal_issn: path of the base portal_issn (in csv format)
    :return: a dict issn2issnl
    """
    ps = [d.split('\t') for d in open(path_file_portal_issn)]
    dict_i2l = {}
    for i in ps[1:]:
        if i[1] not in dict_i2l:
            if i[0] != '':
                dict_i2l[i[1].replace('-', '').upper()] = i[0].replace('-', '').upper()
            else:
                dict_i2l[i[1].replace('-', '').upper()] = i[1].replace('-', '').upper()
    return dict_i2l


def read_base(base_name: str, issn2issnl: dict, mode='create_base'):
    """
    Reads the attributes of a index base
    :param issn2issnl: a dict where each key is a issn and each value is a issn-l
    :param base_name: the name of the index base
    :param mode: the mode of exectution: create_base to create a base and (ii) count to count the number of char's ocurrences
    :return: a dict where each key is a issn-l and each value is a list of one list of issns and one list of titles
    """
    dict_base = {}
    num_ignored_lines = 0

    col_issnl = BASE2COLUMN_INDEXES.get(base_name).get('issnl')
    cols_issn = BASE2COLUMN_INDEXES.get(base_name).get('issn')
    cols_title = BASE2COLUMN_INDEXES.get(base_name).get('title')
    base_sep = BASE2COLUMN_INDEXES.get(base_name).get('sep')

    base_data = open(DEFAULT_DIR_INDEXES + base_name + '.csv')

    # ignore first line
    base_data.readline()

    line = base_data.readline()

    if mode == 'count':
        all_original_titles = []

    print('reading base %s' % base_name)

    while line:
        i = line.split(base_sep)

        issns = [i[j].strip().upper() for j in cols_issn if i[j].strip() != '' and is_valid_issn(i[j].strip())]
        issns = list(set([x.replace('-', '') for x in issns if x != '****-****']))

        if has_valid_issn(issns):
            if col_issnl is None:
                if len(issns) > 0:
                    issnl = get_issnl_from_dict(issns, issn2issnl)
            else:
                issnl = get_issnl_from_base(issns, col_issnl)

            if issnl is not None:
                titles = list(set([StringProcessor.preprocess_journal_title(i[j].strip(), include_parenthesis_info=True) for j in cols_title]))
                titles.extend(list(set([StringProcessor.preprocess_journal_title(i[j].strip()) for j in cols_title])))
                titles = list(set([t.upper() for t in titles if is_valid_title(t)]))

                if mode == 'count':
                    titles = list(set([i[j].strip() for j in cols_title if is_valid_title(i[j].strip())]))
                    all_original_titles.extend(titles)

                if issnl != '' and len(titles) > 0:
                    if issnl not in dict_base:
                        dict_base[issnl] = [issns, titles]
                    else:
                        dict_base[issnl][0].extend(issns)
                        dict_base[issnl][0] = list(set(dict_base[issnl][0]))
                        dict_base[issnl][1].extend(titles)
                        dict_base[issnl][1] = list(set(dict_base[issnl][1]))
            else:
                num_ignored_lines += 1
        else:
            num_ignored_lines += 1

        line = base_data.readline()

    if mode == 'count':
        return all_original_titles

    print('\tlines ignored %d' % num_ignored_lines)
    return dict_base


def merge_bases(bases):
    """
    Merges all the bases into one base
    :param bases: a list of bases
    :return: the merged version of the bases
    """
    merged_bases = {}
    for b in bases:
        for k, v in b.items():
            if k not in merged_bases:
                merged_bases[k] = v
            else:
                existant_issns = merged_bases[k][0]
                new_issns = v[0]
                existant_titles = merged_bases[k][1]
                new_titles = v[1]

                merged_bases[k][0] = list(set(existant_issns).union(set(new_issns)))
                merged_bases[k][1] = list(set(existant_titles).union(set(new_titles)))
    return merged_bases


def save_bases(merged_bases):
    """
    Saves the merged bases into the disk
    :param merged_bases: a dictionary representing a merged base
    """
    final_base = open(DEFAULT_DIR_INDEXES + '../base_issnl2all.csv', 'w')
    for k in sorted(merged_bases.keys()):
        v = merged_bases.get(k)
        j = v[0]
        t = v[1]
        final_base.write('\t'.join([k] + ['#'.join(vj for vj in j)] + ['#'.join(vt for vt in t)]) + '\n')
    final_base.close()

    final_title2issnl = open(DEFAULT_DIR_INDEXES + '../base_titulo2issnl.csv', 'w')
    title2issnl = {}
    for k in sorted(merged_bases.keys()):
        v = merged_bases.get(k)
        t = v[1]
        for ti in t:
            if ti not in title2issnl:
                title2issnl[ti] = [k]
            else:
                title2issnl[ti].append(k)
    for title in sorted(title2issnl):
        final_title2issnl.write('%s\t%s' % (title, '#'.join(title2issnl.get(title))) + '\n')
    final_title2issnl.close()


if __name__ == '__main__':
    logging.basicConfig(filename='merge_indexes.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if len(sys.argv) == 2:
        DEFAULT_DIR_INDEXES = sys.argv[1]

    if len(sys.argv) == 3:
        DEFAULT_DIR_INDEXES = sys.argv[1]
        DEFAULT_MODE = sys.argv[2]

    issn2issnl = mount_issn2issnl_dict(DEFAULT_DIR_INDEXES + 'portal_issn.csv')

    bases_names = ['doaj', 'latindex', 'portal_issn', 'scielo', 'scimago_jr', 'scopus', 'ulrich', 'wos_jcr']

    if DEFAULT_MODE == 'create_base':
        bases = []
        for b in bases_names:
            bases.append(read_base(b, issn2issnl))
        merged_bases = merge_bases(bases)
        save_bases(merged_bases)
    else:
        titles = []
        for b in bases_names:
            titles.extend(read_base(b, issn2issnl, mode='count'))
        char2freq = check_char_freq(titles)
        save_char_freq(char2freq)
