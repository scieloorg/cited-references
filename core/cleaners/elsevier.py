import argparse
import csv
import core.util.file as file
import re

from scielo_scholarly_data import standardizer


ELSEVIER_KEYS = [
    'sourcetitle',
    'pubyear',
    'volume',
    'issn_print',
    'issn_electronic',
    'citationcount',
    'doiset',
]

PATTERNS_DOI = [
    re.compile(pd) for pd in [
        r'^10.\d{4,9}/[-._;()/:A-Za-z0-9]+',
        r'10.\d{4,9}/[-._;()/:A-Z0-9]+$',
        r'10.1002/[^\s]+$',
        r'10.\d{4}/\d+-\d+X?(\d+)\d+<[\d\w]+:[\d\w]*>\d+.\d+.\w+;\d$',
        r'10.1207/[\w\d]+\&\d+_\d+$',
    ]
]


def std_doi(text):
    for pattern_doi in PATTERNS_DOI:
        matched_doi = pattern_doi.search(text)
        if matched_doi:
            return matched_doi.group()


def _clean(data):
    cleaned_data = {}

    cleaned_data.update({
        'sourcetitle': standardizer.journal_title_for_deduplication(data.get('sourcetitle', '')),
        'pubyear': standardizer.document_publication_date(data.get('pubyear', ''), only_year=True),
        'volume': standardizer.issue_volume(data.get('volume', '')),
        'citationcount': data.get('citationcount', ''),
        'issn_print': standardizer.journal_issn(data.get('issn_print', ''), use_issn_validator=True),
        'issn_electronic': standardizer.journal_issn(data.get('issn_electronic', ''), use_issn_validator=True)
    })

    doiset = data.get('doiset', '')
    if doiset:
        doiset_list = [std_doi(doi) for doi in doiset.split(' ') if doi]
        cleaned_doiset = ';'.join([d for d in doiset_list if d])
        cleaned_data.update({'doiset': cleaned_doiset})
    else:
        cleaned_data.update({'doiset': ''})

    return cleaned_data


def write_refs_to_csv(path: str, data: dict):
    with open(path, 'w') as fout:
        csv_writer = csv.DictWriter(fout, fieldnames=['id'] + ELSEVIER_KEYS)
        csv_writer.writeheader()
        csv_writer.writerows(data)


def write_issn_to_csv(path: str, issn: str):
    with open(path, 'a') as fout:
        fout.write(issn + '\n')


def write_dois_to_csv(path: str, dois: list):
    with open(path, 'w') as fout:
        for doi in dois:
            fout.write(doi + '\n')


def _generate_output_path(path: str, posfix: str):
    if path.endswith('cleaned'):
        return path.replace('cleaned', posfix)
    return path + '_' + posfix


def export_issns(path, data):
    issns = []

    for d in data:
        for k in ['issn_electronic', 'issn_print']:
            issn = d.get(k)

            if issn and issn not in issns:
                issns.append(issn)
                write_issn_to_csv(path, issn)


def export_dois(path, data):
    dois = set()

    for d in data:
        for doi in d.get('doiset', '').split(';'):
            if doi and doi not in dois:
                dois.add(doi)

    write_dois_to_csv(path, sorted(dois))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', required=True)
    parser.add_argument('--delimiter', default=',')
    parser.add_argument('--mode', choices=['clean', 'doi', 'issn'], required=True)

    args = parser.parse_args()

    if args.mode == 'clean':
        cleaned_data = []

        line_number = 1
        for i in file.load_refs_from_csv(args.path, args.delimiter):
            cleaned_line = _clean(i)
            cleaned_line.update({'id': line_number})
            cleaned_data.append(cleaned_line)
            line_number += 1

        write_refs_to_csv(args.path + '_cleaned', cleaned_data)

    if args.mode == 'doi':
        data = file.load_refs_from_csv(args.path, args.delimiter)
        output = _generate_output_path(args.path, 'doi')
        export_dois(output, data)

    if args.mode == 'issn':
        data = file.load_refs_from_csv(args.path, args.delimiter)
        output = _generate_output_path(args.path, 'issn')
        export_issns(output, data)
