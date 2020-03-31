import json

from pymongo import MongoClient


attributes = ['date-parts',             # list of list [[yyyy, m, dd]]
                  'publisher-location',     # str
                  'publisher',              # str
                  'short-container-title',  # list of str
                  'doi',                    # str
                  'type',                   # str
                  'title',                  # list of str
                  'author',                 # list of dicts {'given': str, 'family': str, 'sequence': str}
                  'event',                  # list of dicts {'name': str, 'location': str, 'sponsor':[str], 'acronym': 'str', 'number': 'str', 'start': {'date-parts':[[]]}, 'end': {'date-parts': [[]]}}
                  'container-title',        # list of str
                  'original-title',         # list
                  'subtitle',               # list
                  'short-title',            # list
                  'ISBN',                   # list of str
                  ]


def mount_id(collection, ref_id):
    els = ref_id.split('_')
    pid = els[0]
    index_number = els[1]
    zeros = 5 - len(index_number)

    middle_id = ''.join(['0' for i in range(zeros)])
    middle_id += str(index_number)

    return '-'.join([pid + middle_id, collection])


def main():
    client = MongoClient()

    ref_db = client['ref_scielo']

    for col in ['arg', 'bol', 'chl', 'cic', 'col', 'cri', 'cub', 'ecu', 'esp', 'mex', 'per', 'prt', 'pry', 'psi', 'rve', 'rvt', 'scl', 'spa', 'sss', 'sza', 'ury', 'ven', 'wid']:

        # Get all the documents that have crossref metadata (status 1)
        pairs = {'metadata': {}}

        for p in ref_db[col].find({'status': 1}):
            p_data = p.get('crossref_metadata', {})
            p_id = p.get('_id')

            new_id = mount_id(col, p_id)

            pairs['metadata'][new_id] = p_data

        json.dump(pairs, open('/home/rafael/crossref_' + col + '.json', 'w'), indent='\t', sort_keys=True)


if __name__ == '__main__':
    main()
