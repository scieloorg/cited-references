import argparse
import csv
import re

from file import (
    check_dir,
    generate_folder_name,
    load_issnl_to_all,
    load_title_to_issnl,
    load_year_volume,
    open_files,
    close_files,
)
from scielo_scholarly_data import standardizer


class CitationRow:
    keys = [
        'pid',
        'journal_title',
        'issn_print',
        'issn_electronic',
        'volume',
        'year',
    ]

    def __init__(self, data:dict):
        for k in CitationRow.keys:
            v = data.get(k, '')
            setattr(self, k, v)

    def __str__(self) -> str:
        return '|'.join([k + ':' + getattr(self, k) for k in CitationRow.keys])


def fuzzy_match(title: str, data: dict):
    words = title.split(' ')

    cleaned_title = standardizer.journal_title_for_deduplication(title)

    if len(cleaned_title) > 6 and len(words) >= 2:
        pattern = r'[\w|\s]*'.join([word for word in words]) + '[\w|\s]*'

        title_pattern = re.compile(pattern, re.UNICODE)
        
        fuzzy_matches = []
        
        for oficial_title in [ot for ot in data.keys() if ot.startswith(words[0])]:
            match = title_pattern.fullmatch(oficial_title)
            
            if match:
                fuzzy_matches.append(data[oficial_title])
        
        return set(fuzzy_matches)
    
    return set()


def update_titles(cited_title, data):
    if cited_title not in data:
        data[cited_title] = 1
    else:
        data[cited_title] += 1


def extract_citation_data(citation, data):
    cit_journal_title_cleaned = standardizer.journal_title_for_deduplication(citation.journal_title).upper()

    update_titles(cit_journal_title_cleaned, data)

    cit_year_cleaned = standardizer.document_publication_date(citation.year, only_year=True)
    cit_volume_cleaned = standardizer.issue_volume(citation.volume)

    return cit_journal_title_cleaned, cit_year_cleaned, cit_volume_cleaned


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--title_to_issnl',
        required=True,
    )

    parser.add_argument(
        '--issn_to_issnl',
        required=True,
    )

    parser.add_argument(
        '--title_year_volume_to_issn',
        required=True,
    )

    parser.add_argument(
        '--use_fuzzy',
        action='store_true',
    )

    parser.add_argument(
        '--output',
        default='matches',
        help='diretório de armazenamento dos resultados'
    )

    parser.add_argument(
        '--input',
        required=True,
        help='arquivo de referências citadas a serem tratadas'
    )

    parser.add_argument(
        '--input_delimiter_primary',
        default='|'
    )

    parser.add_argument(
        '--input_delimiter_secondary',
        default='#',
    )

    params = parser.parse_args()

    title2issnl = load_title_to_issnl(params.title_to_issnl)
    issn2issnl, issn2titles = load_issnl_to_all(params.issn_to_issnl)
    title_year_volume2issn = load_year_volume(params.title_year_volume_to_issn, issn2issnl)

    results_dir = generate_folder_name(params.output)
    check_dir(results_dir)
    d_results = open_files(results_dir)

    d_titles = {}
    d_matched_titles = {}
    d_unmatched_titles = {}

    pid_counter = 0

    with open(params.input) as fin:
        csv_reader = csv.DictReader(fin, delimiter=params.input_delimiter_primary)
        
        for row in csv_reader:
            pid_counter += 1
            if pid_counter % 1000 == 0:
                print(pid_counter)

            cit = CitationRow(row)
            cit.pid = str(pid_counter)
            
            cit_journal_title_cleaned, cit_year_cleaned, cit_volume_cleaned = extract_citation_data(cit, d_titles)

            if cit_journal_title_cleaned not in d_titles:
                d_titles[cit_journal_title_cleaned] = 1
            else:
                d_titles[cit_journal_title_cleaned] += 1

            # exato
            if cit_journal_title_cleaned in title2issnl:
                res_issns = title2issnl.get(cit_journal_title_cleaned)
                
                res_line = [
                    cit.pid, 
                    cit_journal_title_cleaned, 
                    res_issns, 
                    str(len(res_issns.split(params.input_delimiter_secondary)))
                ]
                
                d_results['matches'].write('\t'.join(res_line) + '\n')

                res_issns_els = res_issns.split(params.input_delimiter_secondary)

                if cit_journal_title_cleaned not in d_matched_titles:
                    d_matched_titles[cit_journal_title_cleaned] = {len(res_issns_els): 1}
                else:
                    if len(res_issns_els) not in d_matched_titles[cit_journal_title_cleaned]:
                        d_matched_titles[cit_journal_title_cleaned][len(res_issns_els)] = 1
                    else:
                        d_matched_titles[cit_journal_title_cleaned][len(res_issns_els)] += 1

                if len(res_issns_els) > 1:
                    if cit_year_cleaned is not None and cit_volume_cleaned is not None:
                        cit_mkey = '-'.join([
                            cit_journal_title_cleaned, 
                            str(cit_year_cleaned), 
                            str(cit_volume_cleaned)
                        ])

                        if cit_mkey in title_year_volume2issn:
                            bsec_cit_issns = title_year_volume2issn[cit_mkey]

                            if len(bsec_cit_issns) == 1:
                                unique_issnl = list(bsec_cit_issns)[0]
                                d_results['homonymous-disambiguated'].write('\t'.join(res_line + [cit_mkey, unique_issnl, str(len(bsec_cit_issns))]) + '\n')                            
                            else:
                                multiple_issnl = list(bsec_cit_issns)
                                d_results['homonymous-disambiguated'].write('\t'.join(res_line + [cit_mkey] + ['#'.join(multiple_issnl), str(len(multiple_issnl))]) + '\n')

            else:
                if params.use_fuzzy:
                    # aproximado
                    if cit_volume_cleaned is not None and cit_volume_cleaned != '' and cit_year_cleaned is not None and cit_year_cleaned != '':
                        fmatches = fuzzy_match(cit_journal_title_cleaned, title2issnl)
                        
                        if len(fmatches) > 0:
                            fres = [
                                cit.pid, 
                                cit_journal_title_cleaned, 
                                str(cit_year_cleaned), 
                                str(cit_volume_cleaned), 
                                '#'.join(fmatches), str(len(fmatches))
                            ]
                            
                            result_line = '|'.join(fres)

                            d_results['fuzzy'].write(result_line + '\n')
                else:
                    if not cit_year_cleaned:
                        cit_year_cleaned = ''
                    if not cit_volume_cleaned:
                        cit_volume_cleaned = ''
                    
                    ftodo = '|'.join([
                        cit.pid, 
                        cit_journal_title_cleaned, 
                        str(cit_year_cleaned), 
                        str(cit_volume_cleaned)
                    ])
                    
                    d_results['fuzzy-todo'].write(ftodo + '\n')

                # sem correspondência
                if cit_journal_title_cleaned not in d_unmatched_titles:
                    d_unmatched_titles[cit_journal_title_cleaned] = 1
                else:
                    d_unmatched_titles[cit_journal_title_cleaned] += 1

    for t in sorted(d_titles, key=lambda x: d_titles[x], reverse=True):
        d_results['journal-titles'].write('\t'.join([
            t, 
            str(d_titles[t])
        ]) + '\n')

    for t in d_matched_titles:
        for k in sorted(d_matched_titles[t], key=lambda y: d_matched_titles[t][y], reverse=True):
            d_results['matched-issns'].write('\t'.join([
                t, 
                str(k), 
                str(d_matched_titles[t][k])
            ]) + '\n')

    for k in sorted(d_unmatched_titles, key=lambda g: d_unmatched_titles[g], reverse=True):
        d_results['unmatched-titles'].write('\t'.join([
            k, 
            str(d_unmatched_titles[k])
        ]) + '\n')

    close_files(d_results)


if __name__ == '__main__':
    main()
