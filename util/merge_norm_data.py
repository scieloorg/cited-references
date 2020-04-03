import json
import os
import zipfile


def normalize_issn(issn):
    if len(issn) == 8:
        return issn[:4] + '-' + issn[4:]


def add_norm_status(d, norm_status):
    new_d = {'metadata': {}}
    for k in d['metadata']:
        v_id = d['metadata'][k]
        if not isinstance(v_id, list):
            v_id['normalization-status'] = norm_status
            new_d['metadata'][k] = v_id
    return new_d


def add_bc1_data(d, i2l, l2is, l2t):
    for k in d['metadata']:
        v_id = d['metadata'][k]
        issn = v_id.get('ISSN', [])
        issn_ls = set()
        for i in issn:
            issn_ls.add(i2l.get(i))

        if len(issn_ls) == 1:
            issn_l = issn_ls.pop()
            issns = l2is.get(issn_l, [])
            titles = l2t.get(issn_l, [])

            v_id['BC1-ISSNS'] = issns
            v_id['BC1-JOURNAL-TITLES'] = titles

    return d


def load_db1(file_name):
    issn2issnl = {}
    issnl2titles = {}
    issnl2issns = {}

    f = open(file_name)
    for line in f:
        els = line.strip().split('|')
        issnl = normalize_issn(els[0])
        issns = [normalize_issn(i) for i in els[1].split('#')]
        titles = els[2].split('#')

        for i in issns:
            issn2issnl[i] = issnl

        issnl2titles[issnl] = titles
        issnl2issns[issnl] = issns

    f.close()

    return issn2issnl, issnl2issns, issnl2titles


def merge_dicts(d1, d2):
    d12 = {'metadata': d1['metadata']}

    for k in d2['metadata']:
        v_id = d2['metadata'][k]
        if k not in d12['metadata']:
            d12['metadata'][k] = v_id
        else:
            print('%s ja esta no dict' % k)

    return d12


def main():

    collections = ['arg', 'bol' 'chl', 'cic', 'col', 'cri', 'cub', 'ecu', 'esp', 'mex', 'per', 'prt', 'pry', 'psi',
                   'rve', 'rvt', 'scl', 'spa', 'sss', 'sza', 'ury', 'ven', 'wid']

    i2l, l2is, l2t = load_db1('/home/rafael/bc1_0.4.csv')

    for col in collections:

        try:
            fcrossref = open('/home/rafael/matches-2019-12/crossref/crossref_' + col + '.json')
            fexact_db1 = open('/home/rafael/matches-2019-12/exact_db1/exact_db1_' + col + '.json')
            fexact_db2 = open('/home/rafael/matches-2019-12/exact_db2/exact_db2_' + col + '.json')
            ffuzzy_db1 = open('/home/rafael/matches-2019-12/fuzzy_db1/fuzzy_db1_' + col + '.json')

            d1 = json.load(fcrossref)
            d2 = json.load(fexact_db1)
            d3 = json.load(fexact_db2)
            d4 = json.load(ffuzzy_db1)

            norm_d1 = add_norm_status(d1, '1')
            norm_d2 = add_norm_status(d2, '2')
            norm_d3 = add_norm_status(d3, '3')
            norm_d4 = add_norm_status(d4, '4')

            norm_d2 = add_bc1_data(norm_d2, i2l, l2is, l2t)
            norm_d3 = add_bc1_data(norm_d3, i2l, l2is, l2t)
            norm_d4 = add_bc1_data(norm_d4, i2l, l2is, l2t)

            d12 = merge_dicts(norm_d1, norm_d2)
            d123 = merge_dicts(d12, norm_d3)
            d1234 = merge_dicts(d123, norm_d4)

            json.dump(d1234, open('/home/rafael/matches-2019-12/external_metadata_' + col + '.json', 'w'))

            zip_file = zipfile.ZipFile('/home/rafael/matches-2019-12/external_metadata_' + col + '.json.zip', 'w')
            zip_file.write('/home/rafael/matches-2019-12/external_metadata_' + col + '.json', 'external_metadata_' + col + '.json', compress_type=zipfile.ZIP_DEFLATED)
            zip_file.close()
            os.remove('/home/rafael/matches-2019-12/external_metadata_' + col + '.json')
        except FileNotFoundError as e:
            print(e)


if __name__ == '__main__':
    main()
