import logging
import sys

from util.string_processor import StringProcessor


BASE2COLUMN_INDEXES = {
        'doaj': {
            'issn': [3, 4],
            'title': [0, 2],
            'sep': ','
        },
        'latindex': {
            'issn-l': 8,
            'issn': [7],
            'title': [14, 22],
            'sep': ';'
        },
        'portal_issn': {
            'issn': [0, 5, 6, 13],
            'title': [1, 7],
            'sep': ','
        },
        'scielo': {
            'issn': [1, 2, 3],
            'title': [4, 5, 6, 8, 10, 11],
            'sep': '\t'
        },
        'scimago_jr': {
            'issn': [4],
            'title': [2],
            'sep': ';'
        },
        'scopus': {
            'issn': [1, 2],
            'title': [0],
            'sep': '\t'
        }, 
        'ulrich': {
            'issn': [4],
            'title': [5, 6],
            'sep': '\t'
        },
        'wos': {
            'issn': [5, 6],
            'title': [2],
            'sep': '\t'
        },
        'wos_jcr': {
            'issn': [3],
            'title': [1, 2],
            'sep': ','
        }
}


def is_issn(text: str):
    text = text.replace('-', '')
    if len(text) == 8:
        return True
    return False


def split_issns(issn: str):
    if ',' in issn:
        els = issn.replace('"', '').strip().split(',')
        return list(set([e.strip() for e in els if e.strip() != '']))

    if 'E-ISSN:' in issn:
        els = issn.replace('"', '').strip().split('E-ISSN:')
        return list(set([e.strip() for e in els if e.strip() != '']))

    if 'ISSN:' in issn:
        els = issn.replace('"', '').strip().split('ISSN:')
        return list(set([e.strip() for e in els if e.strip() != '']))


def has_issn(issns: list):
    for i in issns:
        if len(i) > 0:
            return True
    return False


def read_base(base_name: str):
    dict_base = {}
    num_ignored_lines = 0

    index_issn_l = BASE2COLUMN_INDEXES.get(base_name).get('issn-l')
    indexes_issn = BASE2COLUMN_INDEXES.get(base_name).get('issn')
    indexes_title = BASE2COLUMN_INDEXES.get(base_name).get('title')
    base_sep = BASE2COLUMN_INDEXES.get(base_name).get('sep')

    base_data = open(DEFAULT_DIR_INDEXES + base_name + '.csv')

    # ignore first line
    base_data.readline()

    line = base_data.readline()

    print('reading base %s' % base_name)

    while line:
        i = line.split(base_sep)

        try:
            issns = []
            if base_name in ['wos', 'wos_jcr', 'scimago_jr']:
                for j in indexes_issn:
                    if ',' in i[j] or 'E-ISSN:' in i[j] or 'ISSN:' in i[j]:
                        issns.extend(split_issns(i[j]))
                    else:
                        issns.append(i[j].replace('"', ''))
            else:
                issns.extend([i[j].strip() for j in indexes_issn if i[j].strip() != '' and is_issn(i[j].strip())])
            issns = list(set([x.replace('-', '') for x in issns if x != '****-****']))
        except IndexError:
            t = line.count('\t')
            print(line, t)

        if index_issn_l is not None:
            issn_l = i[index_issn_l]
        elif len(issns) > 0:
            issn_l = i2l.get(issns[0], '')
            if issn_l == '':
                issn_l = issns[0]

        if has_issn(issns):
            titles = list(set([StringProcessor.preprocess_journal_title(i[j].strip().replace('"', '')) for j in indexes_title if i[j].strip() != '']))

            if issn_l != '':
                if issn_l not in dict_base:
                    dict_base[issn_l] = [issns, titles]
                else:
                    # TODO: it is necessary to unify each list contents
                    dict_base[issn_l][0].extend(issns)
                    dict_base[issn_l][0] = list(set(dict_base[issn_l][0]))
                    dict_base[issn_l][1].extend(titles)
                    dict_base[issn_l][1] = list(set(dict_base[issn_l][1]))
        else:
            num_ignored_lines += 1

        line = base_data.readline()

    print('\tlines ignored %d' % num_ignored_lines)
    return dict_base


def merge_issns(existant_issns: list, new_issn: str):
    if new_issn not in existant_issns:
        existant_issns.append(new_issn)
    return existant_issns


def merge_titles(existant_titles: list, new_title: str):
    if new_title not in existant_titles:
        existant_titles.append(new_title)
    return existant_titles


def merge_bases(bases):
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
    final_base = open(DEFAULT_DIR_INDEXES + 'base_de_correcao.csv', 'w')
    for k in sorted(merged_bases.keys()):
        v = merged_bases.get(k)
        j = v[0]
        t = v[1]
        final_base.write('\t'.join([k] + ['#'.join(vj for vj in j)] + ['#'.join(vt for vt in t)]) + '\n')
    final_base.close()


DEFAULT_DIR_INDEXES = 'bases/'

if __name__ == '__main__':
    logging.basicConfig(filename='merge_indexes.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if len(sys.argv) == 2:
        DEFAULT_DIR_INDEXES = sys.argv[1]

    i2l = {}
    fileiel = open(DEFAULT_DIR_INDEXES + 'i2l.txt')
    fileiel.readline()
    for l in fileiel:
        els = l.strip().split('\t')
        i2l[els[0]] = els[1]

    bases_names = ['doaj', 'latindex', 'portal_issn', 'scielo', 'scimago_jr', 'scopus', 'ulrich', 'wos_jcr'] #'wos', 'wos_jcr']
    bases = []
    for b in bases_names:
        bases.append(read_base(b))
    merged_bases = merge_bases(bases)
    save_bases(merged_bases)
