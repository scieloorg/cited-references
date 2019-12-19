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
    title_year_volume2issn = {}
    for r in year_volume_base:
        title = r[1].strip()
        issn = r[0].strip().replace('-', '')
        normalized_issn = issn2issnl.get(issn, '')
        if normalized_issn == '':
            normalized_issn = issn
        year = r[2].strip()
        volume = r[3].strip()
        mkey = '-'.join([title, year, volume])
        if mkey not in title_year_volume2issn:
            title_year_volume2issn[mkey] = {normalized_issn}
        else:
            title_year_volume2issn[mkey].add(normalized_issn)
    del year_volume_base
    return title_year_volume2issn


def validate(match, yv2issln, l2titles):
    col = match[0]
    id_ref = match[1]
    title = match[2]
    year = match[3]
    volume = match[4]
    issnls = match[5].split('#')
    n = match[6]

    vs = set()
    for l in issnls:
        # l_issns = l2issns[l]
        l_titles = l2titles[l]
        # for i in l_issns:
        for t in l_titles:
            k = '|'.join([t, year, volume])
            if k in yv2issln:
                vs.add(yv2issln[k])
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
    yv2issnl = load_year_volume_base(path_year_volume, i2l)
    del i2l

    results = []
    for i, d in enumerate(data):
        print('\r%d/%d' % (i + 1, len(data)), end='')
        results.append([d[:3], validate(d, yv2issnl, l2titles)])
    save_results(results)


if __name__ == '__main__':
    main()
