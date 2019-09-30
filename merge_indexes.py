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


def has_valid_issn(issns: list):
    """
    Checks if a list of issns have at least one valid issn
    :param issns: a list of issns
    :return: True if the list contains at least one valid issn, False otherwise
    """
    for i in issns:
        if len(i.replace('-', '')) == 8:
            return True
    return False


def has_valid_title(titles: list):
    """
    Checks if a list of titles have at least one valid title
    :param titles: a list of titles
    :return: True if the list contains at least one valid title, False otherwise
    """
    for t in titles:
        if len(t) > 0:
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


def read_base(base_name: str, mode='create_base'):
    """
    Reads the attributes of a index base
    :param base_name: the name of the index base
    :param mode: the mode of exectution: create_base to create a base and (ii) count to count the number of char's ocurrences
    :return: a dict where each key is a issn-l and each value is a list of one list of issns and one list of titles
    """
    dict_base = {}
    num_ignored_lines = 0

    index_issn_l = BASE2COLUMN_INDEXES.get(base_name).get('issn_l')
    indexes_issn = BASE2COLUMN_INDEXES.get(base_name).get('issn')
    indexes_title = BASE2COLUMN_INDEXES.get(base_name).get('title')
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

        try:
            issns = [i[j].strip() for j in indexes_issn if i[j].strip() != '' and is_valid_issn(i[j].strip())]
            issns = list(set([x.replace('-', '') for x in issns if x != '****-****']))
        except IndexError:
            t = line.count('\t')
            print(line, t)

        if index_issn_l is not None and i[index_issn_l] != '':
            issn_l = i[index_issn_l].replace('-', '')
        elif len(issns) > 0:
            for x in sorted(issns):
                issn_l = i2l.get(x, '')
                if issn_l != '':
                    break
            if issn_l == '':
                issn_l = issns[0].replace('-', '')

        if has_valid_issn(issns):
            titles = list(set([StringProcessor.preprocess_journal_title(i[j].strip()) for j in indexes_title]))
            titles = list(set([t.upper() for t in titles if is_valid_title(t)]))

            if mode == 'count':
                titles = list(set([i[j].strip() for j in indexes_title if is_valid_title(i[j].strip())]))
                all_original_titles.extend(titles)

            if issn_l != '' and len(titles) > 0:
                if issn_l not in dict_base:
                    dict_base[issn_l] = [issns, titles]
                else:
                    dict_base[issn_l][0].extend(issns)
                    dict_base[issn_l][0] = list(set(dict_base[issn_l][0]))
                    dict_base[issn_l][1].extend(titles)
                    dict_base[issn_l][1] = list(set(dict_base[issn_l][1]))
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

    i2l = {}
    fileiel = open(DEFAULT_DIR_INDEXES + 'issn2issnl.txt')
    fileiel.readline()
    for l in fileiel:
        els = l.strip().split('\t')
        i2l[els[0]] = els[1]

    bases_names = ['doaj', 'latindex', 'portal_issn', 'scielo', 'scimago_jr', 'scopus', 'ulrich', 'wos_jcr']

    if DEFAULT_MODE == 'create_base':
        bases = []
        for b in bases_names:
            bases.append(read_base(b))
        merged_bases = merge_bases(bases)
        save_bases(merged_bases)
    else:
        titles = []
        for b in bases_names:
            titles.extend(read_base(b, mode='count'))
        char2freq = check_char_freq(titles)
        save_char_freq(char2freq)
