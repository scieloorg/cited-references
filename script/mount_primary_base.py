#!/usr/bin/env python3
import logging
import sys

from util.string_processor import StringProcessor


DEFAULT_DIR_INDEXES = 'bases/'
DEFAULT_MODE = 'create'
DEFAULT_USE_COUNTRY_FROM_DICT = True
DEFAULT_USE_ISSNL_FROM_DICT = True

BASE2COLUMN_INDEXES = {
        'doaj': {
            'issn': [2, 3],
            'title': [0, 1],
            'sep': '\t',
            'country': 4,
        },
        'latindex': {
            'issn': [0],
            'title': [4, 5],
            'sep': '\t',
            'country': 3,
        },
        'nlm': {
            'issn': [3, 4],
            'title': [1, 2, 5],
            'sep': '\t',
        },
        'portal_issn': {
            'issn_l': 0,
            'issn': [1],
            'main_title': 6,
            'main_title_alternative': 2,
            'main_abbrev_title': 3,
            'title': [2, 3, 4, 5, 6, 7, 8],
            'sep': '\t',
            'country': 9,
            'start_year': 10,
            'end_year': 11,
        },
        'scielo': {
            'issn': [1, 2, 3],
            'title': [4, 5, 6],
            'sep': '\t',
            'country': 0,
        },
        'scimago_jr': {
            'issn': [1, 2],
            'title': [0],
            'sep': ';',
            'country': 3,
        },
        'scopus': {
            'issn': [1, 2],
            'title': [0],
            'sep': '\t',
        }, 
        'ulrich': {
            'issn': [1],
            'title': [2],
            'sep': '\t',
            'country': 0,
        },
        'wos': {
            'issn': [1, 2],
            'title': [0],
            'sep': '\t',
            'country': 3,
        },
        'wos_jcr': {
            'issn': [2],
            'title': [0, 1],
            'sep': '\t',
        }
}


def is_valid_issn(issn: str):
    """
    Check if an ISSN is valid
    :param issn: the original issn code
    :return: True if the ISSN is valid, False otherwise
    """
    issn = issn.replace('-', '')
    if len(issn) == 8:
        return True
    return False


def is_valid_title(title: str):
    """
    Check if a title is valid (has at least one char)
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
        vls = set()
        for j in issns:
            if j in issn2issnl.values():
                vls.add(j)
        if len(vls) == 1:
            isl = vls.pop()
            logging.warning('issn %s is the issn-l' % isl)
            return isl
        elif len(vls) == 0:
            logging.warning('%s is not in the list (neither as a key (issn) nor as a value (issn-l)' % str(issns))
            return None
        else:
            logging.warning('issns (%s) are issn-ls and points to multiple issn-ls (vls)' % str(issns, vls))
            return None
    else:
        logging.warning('%s links to multiple issn-l' % issns)
        return None


def has_valid_issn(issns: list):
    """
    Check if a list of issns have at least one valid issn
    :param issns: a list of issns
    :return: True if the list contains at least one valid issn, False otherwise
    """
    for i in issns:
        if is_valid_issn(i):
            return True
    return False


def has_valid_title(titles: list):
    """
    Check if a list of titles have at least one valid title
    :param titles: a list of titles
    :return: True if the list contains at least one valid title, False otherwise
    """
    for t in titles:
        if is_valid_title(t):
            return True
    return False


def check_char_freq(titles: list):
    """
    Count the number of ocurrences of each char present in the titles
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
    Save the char2freq dictionary into the disk
    :param c2freq: a dictionary where each key is a char and each value is composed by the preprocessed version of the char and the char's number of ocurrences
    """
    final_c2freq = open(DEFAULT_DIR_INDEXES + '../char_freq.csv', 'w')
    for k in sorted(c2freq, key=lambda x: c2freq.get(x), reverse=True):
        final_c2freq.write('%s\t%s\t%d' % (k, StringProcessor.preprocess_journal_title(k), c2freq.get(k)) + '\n')
    final_c2freq.close()


def mount_issnl2country_dict(path_file_portal_issn: str):
    """
    Mount a dictionary where each key is an ISSN-L and each value is a country
    :param path_file_portal_issn: path of the base portal_issn (in csv format)
    :return: a dict issnl2country
    """
    col_issnl = BASE2COLUMN_INDEXES.get('portal_issn').get('issn_l')
    cols_issn = BASE2COLUMN_INDEXES.get('portal_issn').get('issn')[0]
    col_country = BASE2COLUMN_INDEXES.get('portal_issn').get('country')
    base_sep = BASE2COLUMN_INDEXES.get('portal_issn').get('sep')

    ps = [d.split(base_sep) for d in open(path_file_portal_issn)]
    dict_il2c = {}
    for i in ps[1:]:
        key_issnl = i[col_issnl].replace('-', '').upper()
        value_issn = i[cols_issn].replace('-', '').upper()
        value_country = i[col_country].strip().upper()
        if key_issnl != '':
            if key_issnl not in dict_il2c:
                dict_il2c[key_issnl] = {'-'.join([value_issn, value_country])}
            else:
                dict_il2c[key_issnl].add('-'.join([value_issn, value_country]))
    return dict_il2c


def mount_issnl2years_dict(path_file_portal_issn: str):
    """
    Mount a dictionary where each key is an ISSN-L and each value is a string composed of start and end years
    :param path_file_portal_issn: path of the base portal_issn (in csv format)
    :return: a dict issnl2years
    """
    col_issnl = BASE2COLUMN_INDEXES.get('portal_issn').get('issn_l')
    cols_issn = BASE2COLUMN_INDEXES.get('portal_issn').get('issn')[0]
    col_start_year = BASE2COLUMN_INDEXES.get('portal_issn').get('start_year')
    col_end_year = BASE2COLUMN_INDEXES.get('portal_issn').get('end_year')
    base_sep = BASE2COLUMN_INDEXES.get('portal_issn').get('sep')

    ps = [d.split(base_sep) for d in open(path_file_portal_issn)]
    dict_il2ys = {}
    for i in ps[1:]:
        key_issnl = i[col_issnl].replace('-', '').upper()
        value_issn = i[cols_issn].replace('-', '').upper()
        value_start_year = i[col_start_year].strip()
        value_end_year = i[col_end_year].strip()
        if key_issnl != '':
            if key_issnl not in dict_il2ys:
                dict_il2ys[key_issnl] = {'-'.join([value_issn, value_start_year, value_end_year])}
            else:
                dict_il2ys[key_issnl].add('-'.join([value_issn, value_start_year, value_end_year]))
    return dict_il2ys


def save_issnl2country_dict(issnl2country: dict):
    """
    Save the issnl2country dictionary into the disk
    :param issnl2country: a dictionary where each key is an ISSN-L and each value is a country
    """
    file_il2country = open(DEFAULT_DIR_INDEXES + '../issnl2country_v0.4.csv', 'w')
    for k in sorted(issnl2country):
        file_il2country.write('%s\t%s' % (k, '#'.join(list(issnl2country[k]))) + '\n')
    file_il2country.close()


def save_issnl2years_dict(issnl2years: dict):
    """
    Save the issnl2years dictionary into the disk
    :param issnl2years: a dictionary where each key is an ISSN-L and each value is the start and end years
    """
    file_il2years = open(DEFAULT_DIR_INDEXES + '../issnl2country_v0.4.csv', 'w')
    for k in sorted(issnl2years):
        file_il2years.write('%s\t%s' % (k, '#'.join(list(issnl2years[k]))) + '\n')
    file_il2years.close()


def mount_issn2issnl_dict(path_file_portal_issn: str):
    """
    Mount a dictionary where each key is an ISSN and each value is a ISSN-L
    :param path_file_portal_issn: path of the base portal_issn (in csv format)
    :return: a dict issn2issnl
    """
    col_issnl = BASE2COLUMN_INDEXES.get('portal_issn').get('issn_l')
    cols_issn = BASE2COLUMN_INDEXES.get('portal_issn').get('issn')
    base_sep = BASE2COLUMN_INDEXES.get('portal_issn').get('sep')

    ps = [d.split(base_sep) for d in open(path_file_portal_issn)]
    dict_i2l = {}
    for i in ps[1:]:
        value_issnl = i[col_issnl].strip().replace('-', '').upper()
        key_issn = i[cols_issn[0]].strip().replace('-', '').upper()

        if key_issn not in dict_i2l:
            if value_issnl != '':
                dict_i2l[key_issn] = value_issnl
            else:
                # assumes that the issn is the issn-l
                dict_i2l[key_issn] = key_issn
        else:
            print('key %s is in the dict already (new value: %s, old value: %s)' % (key_issn, value_issnl, dict_i2l[key_issn]))
    return dict_i2l


def read_base(base_name: str, issn2issnl: dict, mode='create'):
    """
    Read the attributes of a index base
    :param issn2issnl: a dict where each key is a issn and each value is a issn-l
    :param base_name: the name of the index base
    :param mode: the mode of exectution: create_base to create a base and (ii) count to count the number of char's ocurrences
    :return: a dict where each key is a issn-l and each value is a list of one list of issns and one list of titles
    """
    dict_base = {}
    num_ignored_lines = 0

    cols_issn = BASE2COLUMN_INDEXES.get(base_name).get('issn')
    cols_title = BASE2COLUMN_INDEXES.get(base_name).get('title')
    col_country = BASE2COLUMN_INDEXES.get(base_name).get('country')
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
            if len(issns) > 0:
                issnl = get_issnl_from_dict(issns, issn2issnl)

                if issnl is not None:
                    titles = list(set([StringProcessor.preprocess_journal_title(i[j].strip(), remove_parenthesis_info=False) for j in cols_title]))
                    titles.extend(list(set([StringProcessor.preprocess_journal_title(i[j].strip()) for j in cols_title])))
                    titles = list(set([t.upper() for t in titles if is_valid_title(t)]))

                    main_title = ''
                    main_abbrev_title = ''

                    if base_name == 'portal_issn':
                        col_main_title = BASE2COLUMN_INDEXES.get(base_name).get('main_title')
                        col_main_title_alternative = BASE2COLUMN_INDEXES.get(base_name).get('main_title_alternative')
                        main_title = StringProcessor.preprocess_journal_title(i[col_main_title].strip()).upper()
                        if main_title == '':
                            main_title = StringProcessor.preprocess_journal_title(i[col_main_title_alternative].strip()).upper()

                        col_main_abbrev_title = BASE2COLUMN_INDEXES.get(base_name).get('main_abbrev_title')
                        main_abbrev_title = StringProcessor.preprocess_journal_title(i[col_main_abbrev_title].strip()).upper()

                    if not DEFAULT_USE_COUNTRY_FROM_DICT:
                        if col_country is not None:
                            country_name = StringProcessor.preprocess_name(i[col_country].strip().upper())
                            if len(country_name) != 0:
                                countries = {country_name}
                            else:
                                countries = set()
                        else:
                            countries = set()

                    if mode == 'count':
                        titles = list(set([i[j].strip() for j in cols_title if is_valid_title(i[j].strip())]))
                        all_original_titles.extend(titles)

                    if issnl != '' and len(titles) > 0:
                        countries = set()
                        if DEFAULT_USE_COUNTRY_FROM_DICT:
                            countries = issnl2country.get(issnl, set())

                        years = issnl2years.get(issnl, set())

                        if issnl not in dict_base:
                            dict_base[issnl] = [issns, [main_title], [main_abbrev_title], titles, countries, years]
                        else:
                            dict_base[issnl][0].extend(issns)
                            dict_base[issnl][0] = list(set(dict_base[issnl][0]))

                            if main_title not in dict_base[issnl][1]:
                                dict_base[issnl][1].append(main_title)
                            if main_abbrev_title not in dict_base[issnl][2]:
                                dict_base[issnl][2].append(main_abbrev_title)

                            dict_base[issnl][3].extend(titles)
                            dict_base[issnl][3] = list(set(dict_base[issnl][3]))
                            dict_base[issnl][4] = dict_base[issnl][4].union(countries)
                            dict_base[issnl][5] = dict_base[issnl][5].union(years)
                else:
                    num_ignored_lines += 1
        else:
            num_ignored_lines += 1

        line = base_data.readline()

    if mode == 'count-char':
        return all_original_titles

    print('\tlines ignored %d' % num_ignored_lines)
    return dict_base


def merge_bases(bases):
    """
    Merge all the bases into one base
    :param bases: a list of bases
    :return: the merged version of the bases
    """
    merged_bases = {}
    for code_base, b in enumerate(bases):
        for k, v in b.items():
            if k not in merged_bases:
                bases_vector = [0 for x in range(len(BASE2COLUMN_INDEXES.keys()))]
                bases_vector[code_base] = 1
                merged_bases[k] = v + [bases_vector]
            else:
                existant_issns = merged_bases[k][0]
                new_issns = v[0]

                existant_main_title = merged_bases[k][1]
                new_main_title = v[1]

                existant_main_abbrev_title = merged_bases[k][2]
                new_main_abbrev_title = v[2]

                existant_titles = merged_bases[k][3]
                new_titles = v[3]

                existant_countries = merged_bases[k][4]
                new_countries = v[4]

                existant_years = merged_bases[k][5]
                new_years = v[5]

                merged_bases[k][0] = list(set(existant_issns).union(set(new_issns)))
                merged_bases[k][1] = [mt for mt in list(set(existant_main_title).union(set(new_main_title))) if mt != '']
                merged_bases[k][2] = [mta for mta in list(set(existant_main_abbrev_title).union(set(new_main_abbrev_title))) if mta != '']
                merged_bases[k][3] = list(set(existant_titles).union(set(new_titles)))
                merged_bases[k][4] = existant_countries.union(new_countries)
                merged_bases[k][5] = existant_years.union(new_years)
                merged_bases[k][6][code_base] = 1

    return merged_bases


def save_bases(merged_bases):
    """
    Save the merged bases into the disk
    :param merged_bases: a dictionary representing a merged base
    """
    final_base = open(DEFAULT_DIR_INDEXES + '../base_issnl2all_v0.5.csv', 'w')

    base_header = '|'.join(['ISSNL', 'ISSNs', 'MAIN_TITLE', 'MAIN_ABBREV_TITLE', 'OTHER_TITLEs', 'PORTAL_ISSN', 'DOAJ', 'LATINDEX', 'NLM', 'SCIELO', 'SCIMAGO_JR', 'SCOPUS', 'ULRICH', 'WOS', 'WOS_JCR', 'COUNTRIES', 'YEARS'])
    base_header_size = len(base_header.split('|'))
    final_base.write(base_header + '\n')

    for k in sorted(merged_bases.keys()):
        v = merged_bases.get(k)
        j = v[0]
        mt = v[1]
        mat = v[2]
        t = v[3]
        c = v[4]
        y = v[5]

        base_codes = v[6]
        
        base_register = '|'.join([k] + ['#'.join(vmt for vmt in mt)] + ['#'.join(vmat for vmat in mat)] + ['#'.join(vj for vj in j)] + ['#'.join(vt for vt in t)] + [str(ci) for ci in base_codes] + ['#'.join(vc for vc in c)] + ['#'.join([vy for vy in y])])
        base_register_size = len(base_register.split('|'))
        
        if base_header_size != base_register_size:
            print('line %s is invalid' % base_register)

        final_base.write(base_register + '\n')
    final_base.close()

    final_title2issnl = open(DEFAULT_DIR_INDEXES + '../base_title2issnl_v0.5.csv', 'w')

    title_base_header = '|'.join(['TITLE', 'ISSNLs', 'PORTAL_ISSN', 'DOAJ', 'LATINDEX', 'NLM', 'SCIELO', 'SCIMAGO_JR', 'SCOPUS', 'ULRICH', 'WOS', 'WOS_JCR', 'COUNTRIES', 'YEARS'])
    title_base_header_size = len(title_base_header.split('|'))

    final_title2issnl.write(title_base_header + '\n')
    title2issnl = {}
    for k in sorted(merged_bases.keys()):
        v = merged_bases.get(k)
        t = v[3]
        c = v[4]
        y = v[5]

        base_codes = v[6]
        for ti in t:
            if ti not in title2issnl:
                title2issnl[ti] = [[k], c, y, base_codes]
            else:
                title2issnl[ti][0].append(k)
                title2issnl[ti][1] = title2issnl[ti][1].union(c)
                title2issnl[ti][2] = title2issnl[ti][2].union(y)
                past_base_codes = title2issnl[ti][3]
                title2issnl[ti][3] = list(map(max, zip(base_codes, past_base_codes)))

    for title in sorted(title2issnl):
        data_issnls = title2issnl.get(title)[0]
        data_countries = title2issnl.get(title)[1]
        data_years = title2issnl.get(title)[2]
        data_base_codes = [str(i) for i in title2issnl.get(title)[3]]

        title_base_register = '|'.join([title, '#'.join(data_issnls)] + data_base_codes + ['#'.join(data_countries), '#'.join(data_years)])
        title_base_register_size = len(title_base_register.split('|'))

        if title_base_header_size != title_base_register_size:
            print('line %s is invalid' % title_base_register)

        final_title2issnl.write(title_base_register + '\n')
    final_title2issnl.close()


if __name__ == '__main__':
    logging.basicConfig(filename='merge_indexes.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if len(sys.argv) == 2:
        DEFAULT_DIR_INDEXES = sys.argv[1]

    if len(sys.argv) == 3:
        DEFAULT_DIR_INDEXES = sys.argv[1]
        DEFAULT_MODE = sys.argv[2]

    issn2issnl = mount_issn2issnl_dict(DEFAULT_DIR_INDEXES + 'portal_issn.csv')
    issnl2country = mount_issnl2country_dict(DEFAULT_DIR_INDEXES + 'portal_issn.csv')
    issnl2years = mount_issnl2years_dict(DEFAULT_DIR_INDEXES + 'portal_issn.csv')

    bases_names = ['portal_issn', 'doaj', 'latindex', 'nlm', 'scielo', 'scimago_jr', 'scopus', 'ulrich', 'wos', 'wos_jcr']

    if DEFAULT_MODE == 'create':
        bases = []
        for b in bases_names:
            bases.append(read_base(b, issn2issnl))
        merged_bases = merge_bases(bases)
        save_bases(merged_bases)
    elif DEFAULT_MODE == 'count-char':
        titles = []
        for b in bases_names:
            titles.extend(read_base(b, issn2issnl, mode='count-char'))
        char2freq = check_char_freq(titles)
        save_char_freq(char2freq)
    elif DEFAULT_MODE == 'dict-country':
        save_issnl2country_dict(issnl2country)
