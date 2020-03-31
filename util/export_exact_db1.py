import json


def mount_id(collection, ref_id):
    els = ref_id.split('_')
    pid = els[0]
    index_number = els[1]
    zeros = 5 - len(index_number)

    middle_id = ''.join(['0' for i in range(zeros)])
    middle_id += str(index_number)

    return '-'.join([pid + middle_id, collection])


def normalize_issn(issn):
    if len(issn) == 8:
        return issn[:4] + '-' + issn[4:]


def normalize_title(title):
    return title.title()


def main():
    collections = ['arg', 'bol', 'chl', 'cic', 'col', 'cri', 'cub', 'ecu', 'esp', 'mex', 'per', 'prt', 'pry', 'psi', 'rve', 'rvt', 'scl', 'spa', 'sss', 'sza', 'ury', 'ven', 'wid']
    pairs = {}
    for c in collections:
        pairs[c] = {'metadata': {}}

    f = open('/home/rafael/exact_db1.csv')
    for line in f:
        els = line.strip().split('\t')
        col = els[0]
        cit_id = els[1]
        journal_title = els[2]
        issns = els[3].split('#')

        if len(issns) == 1:
            new_id = mount_id(col, cit_id)
            norm_issn = normalize_issn(issns[0])
            norm_title = normalize_title(journal_title)
            pairs[col]['metadata'][new_id] = {'type': 'journal-article',
                                         'ISSN': [norm_issn],
                                         'container-title': [norm_title]}

    for col in collections:
        json.dump(pairs[col], open('/home/rafael/exact_db1_' + col + '.json', 'w'), indent='\t', sort_keys=True)


if __name__ == '__main__':
    main()
