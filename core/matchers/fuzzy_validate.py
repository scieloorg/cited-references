#!/usr/bin/env python3


def read_fuzzy_data(path_file_fuzzy_data):
    fd = open(path_file_fuzzy_data)
    data = [d.strip().split('|') for d in fd]
    return data


def extract_i2_dicts(path_issnl2all_base: str):
    print('extracting issn -> issln, titles dicts')
    base_issnl2all = [i.split('|') for i in open(path_issnl2all_base)]
    i2issnl = {}
    i2titles = {}
    for r in base_issnl2all[1:]:
        issns = r[1].split('#')
        issnl = r[0]
        titles = r[2].split('#')
        for i in issns:
            if i not in i2issnl:
                i2issnl[i] = issnl
                i2titles[i] = titles
            else:
                if i2issnl[i] != issnl:
                    print('ERROR: values (issnls) %s != %s for key (issn) %s' % (issnl, i2issnl[i], i))
    del base_issnl2all
    return i2issnl, i2titles


def extract_l2_dicts(path_issnl2all_base: str):
    print('extracting issnl -> issns, titles dicts')
    base_issnl2all = [i.split('|') for i in open(path_issnl2all_base)]
    l2is = {}
    l2titles = {}
    for r in base_issnl2all[1:]:
        issnl = r[0]
        issns = r[1].split('#')
        titles = r[2].split('#')
        l2is[issnl] = issns
        l2titles[issnl] = titles
    del base_issnl2all
    return l2is, l2titles


def load_year_volume_base_2nd(path_year_volume_base: str, issn2issnl: dict):
    print('loading secondary base')
    yv2 = [i.strip().split('|') for i in open(path_year_volume_base)]
    issn_year_volume2issnl = {}
    for r in yv2:
        issn = r[0].strip().replace('-', '')
        normalized_issnl = issn2issnl.get(issn, '')
        if normalized_issnl == '':
            normalized_issnl = issn
        year = r[2].strip()
        volume = r[3].strip()
        mkey = '|'.join([issn, year, volume])
        if mkey not in issn_year_volume2issnl:
            issn_year_volume2issnl[mkey] = {normalized_issnl}
        else:
            issn_year_volume2issnl[mkey].add(normalized_issnl)
    del yv2
    return issn_year_volume2issnl


def load_year_volume_base_3rd(path_year_volume_base: str):
    print('loading terciary base')
    yv3 = [i.strip().split('|') for i in open(path_year_volume_base)][1:]
    issn_year_volume2issnl = {}
    for r in yv3:
        issnl = r[0]
        issn = r[1]
        year = r[2]
        predicted_vols = [v for v in [r[4], r[5], r[6]] if int(v) > 0]

        for pv in predicted_vols:
            mkey = '|'.join([issn, year, pv])
            if mkey not in issn_year_volume2issnl:
                issn_year_volume2issnl[mkey] = {issnl}
            else:
                issn_year_volume2issnl[mkey].add(issnl)
    del yv3
    return issn_year_volume2issnl


def validate(match, iyv2issnl, l2is):
    year = match[3]
    if len(year) > 4:
        year = year[:4]
    volume = match[4]
    issnls = match[5].split('#')

    vs = set()
    for l in issnls:
        l_issns = l2is[l]
        for i in l_issns:
            k = '|'.join([i, year, volume])
            if k in iyv2issnl:
                v = '#'.join(iyv2issnl[k])
                if len(iyv2issnl[k]) != 1:
                    print(iyv2issnl[k])
                vs.add(v)
    if len(vs) == 0:
        vs = {'-1'}
    return list(vs)


def save_results(res):
    f = open('fuzzy_11_12_tested.csv', 'w')
    f.write('col|ref_id|titulo|year|volume|issns|n_issns|valid_2nd|valid_3rd\n')
    for r in res:
        issnls_2nd = '#'.join(r[1])
        issnls_3rd = '#'.join(r[2])
        fr = '|'.join(r[0] + [issnls_2nd, issnls_3rd])
        f.write(fr + '\n')
    f.close()


def main():
    path_fuzzy = '/home/rafael/Temp/scielo/analysis/2019-12/matches_11_12/fuzzy_11_12.tsv'
    path_issnl2all = '/home/rafael/Temp/scielo/bases/primaria/base_issnl2all_v0.4.csv'
    path_year_volume_2nd = '/home/rafael/Temp/scielo/bases/secundaria/base_year_volume_v0.3.csv'
    path_year_volume_3rd = '/home/rafael/Temp/scielo/bases/terciaria/base_pvol_issn_v0.1.csv'

    data = read_fuzzy_data(path_fuzzy)
    i2l, i2titles = extract_i2_dicts(path_issnl2all)
    del i2titles
    l2is, l2titles = extract_l2_dicts(path_issnl2all)
    iyv2issnl_2nd = load_year_volume_base_2nd(path_year_volume_2nd, i2l)
    del i2l
    iyv2issnl_3rd = load_year_volume_base_3rd(path_year_volume_3rd)

    print('validating with secondary base')
    results = []
    for i, d in enumerate(data):
        print('\r%d/%d' % (i + 1, len(data)), end='')
        results.append([d, validate(d, iyv2issnl_2nd, l2is)])
    print('')

    print('validating with terciary base')
    for i, d in enumerate(results):
        print('\r%d/%d' % (i + 1, len(results)), end='')
        d.append(validate(d[0], iyv2issnl_3rd, l2is))
    print('')

    save_results(results)


if __name__ == '__main__':
    main()
