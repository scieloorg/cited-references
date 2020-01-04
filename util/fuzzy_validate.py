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


def load_year_volume_base(path_year_volume_base: str, issn2issnl: dict):
    print('loading year_volume_base')
    year_volume_base = [i.split('|') for i in open(path_year_volume_base)]
    issn_year_volume2issnl = {}
    for r in year_volume_base:
        # title = r[1].strip()
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
    del year_volume_base
    return issn_year_volume2issnl


def validate(match, iyv2issnl, l2is):
    col = match[0]
    id_ref = match[1]
    # title = match[2]
    year = match[3]
    if len(year) > 4:
        year = year[:4]
    volume = match[4]
    issnls = match[5].split('#')
    # n = match[6]

    vs = set()
    for l in issnls:
        l_issns = l2is[l]
        # l_titles = l2titles[l]
        for i in l_issns:
        # for t in l_titles:
            k = '|'.join([i, year, volume])
            if k in iyv2issnl:
                v = '#'.join(iyv2issnl[k])
                if len(iyv2issnl[k]) != 1:
                    print(iyv2issnl[k])
                vs.add(v)
    return vs


def save_results(res):
    f = open('/home/rafael/valid_matches.csv', 'w')
    for r in res:
        col = r[0][0]
        id_ref = r[0][1]
        title = r[0][2]
        issnls = '#'.join(r[1])
        fr = '|'.join([col, id_ref, title, issnls])
        f.write(fr + '\n')
    f.close()


def main():
    path_fuzzy = '/home/rafael/Temp/scielo/analysis/2019-12/matches_11_12/fuzzy_11_12.tsv'
    path_issnl2all = '/home/rafael/Temp/scielo/bases/primaria/base_issnl2all_v0.4.csv'
    path_year_volume = '/home/rafael/Temp/scielo/bases/secundaria/base_year_volume_v0.3.csv'

    data = read_fuzzy_data(path_fuzzy)
    i2l, i2titles = extract_i2_dicts(path_issnl2all)
    del i2titles
    l2is, l2titles = extract_l2_dicts(path_issnl2all)
    iyv2issnl = load_year_volume_base(path_year_volume, i2l)
    del i2l

    results = []
    for i, d in enumerate(data):
        print('\r%d/%d' % (i + 1, len(data)), end='')
        results.append([d[:3], validate(d, iyv2issnl, l2is)])
    save_results(results)


if __name__ == '__main__':
    main()
