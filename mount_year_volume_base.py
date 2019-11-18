#!/usr/bin/env python3
import json
from util.string_processor import StringProcessor


def get_doi2cited_form_dict(path_refs_wos_doi):
    file_refs_wos_doi = open(path_refs_wos_doi)
    line = file_refs_wos_doi.readline()
    doi2cited_form = {}
    while line:
        rels = line.split('|')
        if len(rels) == 6:
            cited_form = StringProcessor.preprocess_journal_title(rels[2].strip()).upper()
            doi = rels[5].strip()
            if doi not in doi2cited_form:
                doi2cited_form[doi] = [cited_form]
            else:
                if cited_form not in doi2cited_form[doi]:
                    doi2cited_form[doi].append(cited_form)
        else:
            print('line is invalid', line, sep='-->')
        try:
            line = file_refs_wos_doi.readline()
        except UnicodeDecodeError as udc:
            print('UnicodeDecodeError', udc, line, sep='-->')
    file_refs_wos_doi.close()
    return doi2cited_form


def get_cited_forms_with_metadata(path_crossref, doi2cited_form: dict):
    file_crossref = open(path_crossref)
    line = file_crossref.readline()
    cited_forms_with_metadata = set()
    not_collected = 0
    while line:
        json_line = json.loads(line)
        doi = json_line.get('url_searched').replace('https://api.crossref.org/works/', '')

        cited_forms = doi2cited_form.get(doi, [])
        if len(cited_forms) == 0:
            not_collected += 1
        else:
            message = json_line.get('message', {})
            if isinstance(message, dict):
                volume = message.get('volume', '')
                issue = StringProcessor.preprocess_journal_title(message.get('issue', '')).upper()
                print_year = str(message.get('journal-issue', {}).get('published-print', {}).get('date-parts', [['', '']])[0][0])
                online_year = str(message.get('journal-issue', {}).get('published-online', {}).get('date-parts', [['', '']])[0][0])

                issns = message.get('issn-type', [{}])

                print_issn = [i.get('value', '') for i in issns if i.get('type', '') == 'print']
                if len(print_issn) == 0:
                    print_issn = ''
                elif len(print_issn) == 1:
                    print_issn = print_issn[0]
                else:
                    print('there are multiple online issns %s' % str(print_issn))

                online_issn = [i.get('value', '') for i in issns if i.get('type', '') == 'electronic']
                if len(online_issn) == 0:
                    online_issn = ''
                elif len(online_issn) == 1:
                    online_issn = online_issn[0]
                else:
                    print('there are multiple online issns %s' % str(online_issn))

                for cit in cited_forms:
                    if print_issn != '' and cit != '' and print_year != '' and volume != '':
                        metadata_print_str = '|'.join([print_issn, cit, print_year, volume, issue])
                        cited_forms_with_metadata.add(metadata_print_str)
                    if online_issn != '' and cit != '' and online_year != '' and volume != '':
                        metadata_online_str = '|'.join([online_issn, cit, online_year, volume, issue])
                        cited_forms_with_metadata.add(metadata_online_str)
        try:
            line = file_crossref.readline()
        except UnicodeDecodeError as udc:
            print('UnicodeDecodeError', udc, line, sep='-->')
    file_crossref.close()
    return cited_forms_with_metadata


def get_scielo_data(path_scielo):
    scielo_results = set()
    file_scielo = open(path_scielo)
    line = file_scielo.readline()
    while line:
        scielo_results.add(line)
        line = file_scielo.readline()
    return scielo_results


def get_wos_si_source_data(path_wos_si_source):
    file_wos_si_source = open(path_wos_si_source)
    line = file_wos_si_source.readline()
    cited_forms_with_metadata = set()
    while line:
        els = line.split('|')
        if len(els) != 7:
            print('line is invalid', line, sep="-->")
        else:
            issn = els[0]
            cited_form_1 = StringProcessor.preprocess_journal_title(els[1].strip()).upper()
            year = els[2]
            volume = els[3]
            cited_form_2 = StringProcessor.preprocess_journal_title(els[4].strip()).upper()

            if issn != '' and year != '' and volume != '':
                if cited_form_1 != '':
                    metadata_str_1 = '|'.join([issn, cited_form_1, year, volume, ''])
                    cited_forms_with_metadata.add(metadata_str_1)
                if cited_form_2 != '':
                    metadata_str_2 = '|'.join([issn, cited_form_2, year, volume, ''])
                    cited_forms_with_metadata.add(metadata_str_2)

        line = file_wos_si_source.readline()
    file_wos_si_source.close()
    return cited_forms_with_metadata


if __name__ == '__main__':
    PATH_FILE_REFS_WOS_DOI = 'refs_wos_doi.txt'
    PATH_FILE_CROSSREF_RESULTS = 'crossref_results.json'
    PATH_FILE_WOS_SI_SOURCE = 'WoS-refs_SIsource_ISSN.txt'
    PATH_FILE_SCIELO = 'scielo_year_volume.tsv'
    PATH_FILE_RESULTS = 'base_year_volume.csv'

    doi2cited_form = get_doi2cited_form_dict(PATH_FILE_REFS_WOS_DOI)

    crossref_data = get_cited_forms_with_metadata(PATH_FILE_CROSSREF_RESULTS, doi2cited_form)
    wos_data = get_wos_si_source_data(PATH_FILE_WOS_SI_SOURCE)
    scielo_data = get_scielo_data(PATH_FILE_SCIELO)

    cross_and_wos_data = crossref_data.union(wos_data)
    merged_data = cross_and_wos_data.union(scielo_data)

    file_results = open(PATH_FILE_RESULTS, 'w')
    for c in sorted(merged_data):
        file_results.write(c + '\n')
    file_results.close()
