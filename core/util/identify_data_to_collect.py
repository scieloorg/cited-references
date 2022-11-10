import argparse
import core.utils.file as file


def _identify_dois_to_collect(path_data, path_base):
    print('Lendo arquivo de DOIs já coletados...')
    dois = file.load_crossref_results_dois(path_base)
    print('Há %d DOIs coletados' % len(dois))

    missing_dois = set()

    with open(path_data) as fin:
        for row in fin:
            doi = row.strip().lower()

            if doi not in dois:
                missing_dois.add(doi)

    with open(path_data + '_missing', 'w') as fout:
        for md in sorted(missing_dois):
            fout.write(md + '\n')


def _identify_issn_to_collect(path_data, path_base):
    issn_to_issnl, issn_to_titles = file.load_issnl_to_all(path_base)
    missing_issns = set()

    with open(path_data) as fin:
        for row in fin:
            issn = row.strip()

            if issn not in issn_to_issnl:
                missing_issns.add(issn)

    with open(path_data + '_missing', 'w') as fout:
        for mi in sorted(missing_issns):
            fout.write(mi + '\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path_data', required=True)
    parser.add_argument('--path_base', required=True)
    parser.add_argument('--mode', required=True, choices=['issn', 'doi'])

    args = parser.parse_args()

    if args.mode == 'issn':
        _identify_issn_to_collect(args.path_data, args.path_base)

    if args.mode == 'doi':
        _identify_dois_to_collect(args.path_data, args.path_base)
