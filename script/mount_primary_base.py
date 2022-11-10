import argparse
import csv
import logging
import os

from scielo_scholarly_data import standardizer


BASE2COLUMN_INDEXES = {
    'doaj': {
        'issn': [2, 3],
        'title': [0, 1],
        'sep': '\t',
        'country': 4,
    },
    'latindex': {
        'issn': [0, 1],
        'title': [2, 3, 4],
        'sep': '\t',
    },
    'nlm': {
        'issn': [3, 4],
        'title': [1, 2, 5],
        'sep': '\t',
    },
    'portal_issn': {
        'issn': [0, 1],
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
        'title': [4, 5, 6, 7, 8],
        'sep': '|',
    },
    'scimago_jr': {
        'issn': [1, 2, 3, 4],
        'title': [0],
        'sep': '\t',
    },
    'scopus': {
        'issn': [1, 2],
        'title': [0],
        'sep': '\t',
    }, 
    'ulrich': {
        'issn': [0],
        'title': [1],
        'sep': '\t',
    },
    'wos': {
        'issn': [1, 2],
        'title': [0],
        'sep': '\t',
    },
    'wos_jcr': {
        'issn': [2],
        'title': [0, 1],
        'sep': '\t',
    }
}


def is_valid_issn(issn):
    if len(issn.replace('-', '')) == 8:
        return True

    return False


def is_valid_title(title):
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
            logging.warning(f'{isl} é o próprio código ISSN-L')
            return isl

        elif len(vls) == 0:
            logging.warning(f'{issns} não estão no mapa de códigos ISSN')
            return None

        else:
            logging.warning(f'{issns} são códigos ISSN-L e apontam para múltiplos ISSN-L {vls}')
            return None

    else:
        logging.warning(f'{issns} estão relacionados a múltiplos códigos ISSN-L')
        return None


def has_valid_issn(issns: list):
    for i in issns:
        if is_valid_issn(i):
            return True
    return False


def mount_issnl2country_dict(path, issn2issnl):
    col_issn = BASE2COLUMN_INDEXES.get('portal_issn').get('issn')[1]
    col_country = BASE2COLUMN_INDEXES.get('portal_issn').get('country')

    delim = BASE2COLUMN_INDEXES.get('portal_issn').get('sep')

    ps = [d.split(delim) for d in open(path)]
    dict_il2c = {}

    for i in ps[1:]:
        issn = standardizer.journal_issn(i[col_issn])
        issn_l = issn2issnl[issn]
        country = i[col_country].strip().upper()

        if issn_l not in dict_il2c:
            dict_il2c[issn_l] = set()
            
        dict_il2c[issn_l].add('-'.join([issn, country]))

    return dict_il2c


def mount_issnl2years_dict(path, issn2issnl):
    col_issn = BASE2COLUMN_INDEXES.get('portal_issn').get('issn')[1]
    col_start_year = BASE2COLUMN_INDEXES.get('portal_issn').get('start_year')
    col_end_year = BASE2COLUMN_INDEXES.get('portal_issn').get('end_year')

    delim = BASE2COLUMN_INDEXES.get('portal_issn').get('sep')

    ps = [d.split(delim) for d in open(path)]
    dict_il2ys = {}

    for i in ps[1:]:
        issn = standardizer.journal_issn(i[col_issn])
        issn_l = issn2issnl[issn]

        start_year = i[col_start_year].strip()
        end_year = i[col_end_year].strip()

        if issn_l not in dict_il2ys:
            dict_il2ys[issn_l] = set()
            
        dict_il2ys[issn_l].add('-'.join([issn, start_year, end_year]))

    return dict_il2ys


def read_base(directory, base_name, issn2issnl, issnl2country, issnl2years):
    dict_base = {}
    num_ignored_lines = 0

    cols_issn = BASE2COLUMN_INDEXES.get(base_name).get('issn')
    cols_title = BASE2COLUMN_INDEXES.get(base_name).get('title')
    delim = BASE2COLUMN_INDEXES.get(base_name).get('sep')

    base_data = open(os.path.join(directory, base_name + '.csv'))

    # ignore first line
    base_data.readline()

    line = base_data.readline()

    while line:
        i = line.split(delim)

        issns = [x for x in set([standardizer.journal_issn(i[j].strip()) for j in cols_issn]) if x is not None]

        if has_valid_issn(issns):
            if len(issns) > 0:
                issnl = get_issnl_from_dict(issns, issn2issnl)

                if issnl is not None:
                    titles = [standardizer.journal_title_for_deduplication(i[j].strip().lower()) for j in cols_title]
                    titles.extend([standardizer.journal_title_for_deduplication(i[j].strip().lower(), keep_parenthesis_content=True) for j in cols_title])
                    titles = list(set([t.upper() for t in titles if is_valid_title(t)]))

                    main_title = ''
                    main_abbrev_title = ''

                    if base_name == 'portal_issn':
                        col_main_title = BASE2COLUMN_INDEXES.get(base_name).get('main_title')
                        col_main_title_alternative = BASE2COLUMN_INDEXES.get(base_name).get('main_title_alternative')

                        main_title = standardizer.journal_title_for_deduplication(i[col_main_title].strip().lower()).upper()
                        if main_title == '':
                            main_title = standardizer.journal_title_for_deduplication(i[col_main_title_alternative].strip().lower()).upper()

                        col_main_abbrev_title = BASE2COLUMN_INDEXES.get(base_name).get('main_abbrev_title')
                        main_abbrev_title = standardizer.journal_title_for_deduplication(i[col_main_abbrev_title].strip().lower()).upper()

                    if issnl != '' and len(titles) > 0:
                        countries = countries = issnl2country.get(issnl, set())

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

    logging.info(f'Foram ignoradas {num_ignored_lines} linhas')

    return dict_base


def merge_bases(bases):
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


def save_bases(directory, merged_bases):
    if not os.path.exists(directory):
        os.makedirs(directory)

    final_base = open(os.path.join(directory, 'base_issnl2all.csv'), 'w')

    base_header = '|'.join(['ISSNL', 'MAIN_TITLE', 'MAIN_ABBREV_TITLE', 'ISSNs', 'OTHER_TITLEs', 'PORTAL_ISSN', 'DOAJ', 'LATINDEX', 'NLM', 'SCIELO', 'SCIMAGO_JR', 'SCOPUS', 'ULRICH', 'WOS', 'WOS_JCR', 'COUNTRIES', 'YEARS'])
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
            logging.warning(f'Linha {base_register} é inválida')

        final_base.write(base_register + '\n')
    final_base.close()


    final_title2issnl = open(os.path.join(directory, 'base_title2issnl.csv'), 'w')

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
            logging.warning(f'Linha {title_base_register} é inválida')

        final_title2issnl.write(title_base_register + '\n')
    final_title2issnl.close()


def load_issn_map(path, primary_delim='|', secondary_delim='#'):
    issn2issnl = {}

    with open(path) as fin:
        for row in csv.DictReader(fin, delimiter=primary_delim, fieldnames=['ISSN-L', 'ISSNs']):
            issn_l = row['ISSN-L']
            issns = row['ISSNs'].split(secondary_delim)

            for i in issns:
                if i not in issn2issnl:
                    issn2issnl[i] = issn_l
                else:
                    if issn2issnl[i] != issn_l:
                        logging.warning(f'{issn_l} != {issn2issnl[i]} para {i}')
    
    return issn2issnl


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--issn_map',
        required=True,
        help='Arquivo CSV que mapeia um ISSN-L a todos os seus códigos ISSN existentes (formato ISSN-L|ISSN1#ISSN2#ISSN3...)'
    )

    parser.add_argument(
        '--input_dir',
        required=True,
        help='Diretório com fontes de dados pré-processadas em formato CSV'
    )

    parser.add_argument(
        '--output_dir',
        required=True,
        help='Diretório com novas bases geradas'
    )

    parser.add_argument(
        '--loglevel',
        default=logging.INFO,
        help='Escopo de mensagens de log'
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logging.info(f'Carregando mapa de códigos ISSN')
    issn2issnl = load_issn_map(args.issn_map)

    logging.info('Gerando mapa de ISSN-País')
    issnl2country = mount_issnl2country_dict(os.path.join(args.input_dir, 'portal_issn.csv'), issn2issnl)

    logging.info('Gerando mapa de ISSN-Ano')
    issnl2years = mount_issnl2years_dict(os.path.join(args.input_dir, 'portal_issn.csv'), issn2issnl)

    bases_names = ['portal_issn', 'doaj', 'latindex', 'nlm', 'scielo', 'scimago_jr', 'scopus', 'ulrich', 'wos', 'wos_jcr']

    bases = []
    for b in bases_names:
        logging.info(f'Lendo base {b}')
        bases.append(read_base(args.input_dir, b, issn2issnl, issnl2country, issnl2years))

    logging.info('Mesclando bases')
    merged_bases = merge_bases(bases)

    logging.info(f'Gravando bases em {args.output_dir}')
    save_bases(args.output_dir, merged_bases)


if __name__ == '__main__':
    main()
