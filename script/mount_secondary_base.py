import argparse
import csv
import logging
import json
import os

from scielo_scholarly_data import standardizer


def load_proj56_raw(path):
    cited_forms_metadata = set()

    with open(path) as fin:
        creader = csv.DictReader(fin)

        for row in creader:
            year = row.get('ref_pubyear')
            
            try:
                volume = standardizer.issue_volume(row.get('ref_volume'))
            except (standardizer.ImpossibleConvertionToIntError, standardizer.InvalidRomanNumeralError):
                volume = row.get('volume', '')

            print_issn = standardizer.journal_issn(row.get('ref_issn_print'))

            online_issn = standardizer.journal_issn(row.get('ref_issn_electronic'))

            issns = [i for i in [print_issn, online_issn] if i is not None]

            cited_form = standardizer.journal_title_for_deduplication(row.get('ref_sourcetitle_set', '').lower()).upper()

            cited_form_abbrev = standardizer.journal_title_for_deduplication(row.get('ref_sourcetitle_abbrev_set', '').lower()).upper()

            titles = [t for t in [cited_form, cited_form_abbrev] if len(t) > 0]

            if volume and year:
                for i in issns:
                    for t in titles:
                        metadata_str = '|'.join([
                            i, 
                            t, 
                            year, 
                            volume, 
                            '',
                        ])

                        cited_forms_metadata.add(metadata_str)

    return cited_forms_metadata


def load_proj56_crossref(path):
    cited_forms_with_metadata = set()

    with open(path) as fin:
        for line in fin:
            json_line = json.loads(line)

            crossref = json_line.get('crossref')

            if not crossref:
                continue

            try:
                volume = standardizer.issue_volume(crossref.get('volume'))
            except:
                volume = crossref.get('volume', '')

            issns = [standardizer.journal_issn(i.get('value')) for i in crossref.get('issn-type', [])]
            issns = [i for i in issns if i is not None]

            year = str(crossref.get('published', {}).get('date-parts', [['','','',]])[0][0])

            titles = crossref.get('container-title', []) + crossref.get('short-container-title', [])
            titles = set([standardizer.journal_title_for_deduplication(t.lower()).upper() for t in titles])
            titles = [t for t in titles if len(t) > 0]

            if volume and year:
                for i in issns:
                    for t in titles:
                        metadata_str = '|'.join([
                            i, 
                            t, 
                            year, 
                            volume, 
                            '',
                        ])

                        cited_forms_with_metadata.add(metadata_str)

    return cited_forms_with_metadata


def get_doi2cited_form_dict(path_refs_wos_doi):
    file_refs_wos_doi = open(path_refs_wos_doi)

    line = file_refs_wos_doi.readline()

    doi2cited_form = {}
    
    while line:
        rels = line.split('|')

        if len(rels) == 6:
            cited_form = standardizer.journal_title_for_deduplication(rels[2].strip().lower()).upper()

            doi = rels[5].strip().lower()
            
            if doi not in doi2cited_form:
                doi2cited_form[doi] = [cited_form]
            else:
                if cited_form not in doi2cited_form[doi]:
                    doi2cited_form[doi].append(cited_form)

        else:
            logging.warning(f'Linha {line} é inválida')

        try:
            line = file_refs_wos_doi.readline()
        except UnicodeDecodeError:
            logging.error(f'Erro de codificação em linha {line}')

    file_refs_wos_doi.close()

    return doi2cited_form


def get_cited_forms_with_metadata(path_crossref, doi2cited_form: dict):
    file_crossref = open(path_crossref)

    line = file_crossref.readline()

    cited_forms_with_metadata = set()

    not_collected = 0

    while line:
        json_line = json.loads(line)

        doi = json_line.get('url_searched').replace('https://api.crossref.org/works/', '').lower()

        cited_forms = doi2cited_form.get(doi, [])

        if len(cited_forms) == 0:
            not_collected += 1
        else:
            message = json_line.get('message', {})
            if isinstance(message, dict):
                try:
                    volume = str(standardizer.issue_volume(message.get('volume', '')))
                except (standardizer.ImpossibleConvertionToIntError, standardizer.InvalidRomanNumeralError):
                    volume = message.get('volume', '')

                issue = standardizer.journal_title_for_deduplication(message.get('issue', '').lower()).upper()

                print_year = str(message.get('journal-issue', {}).get('published-print', {}).get('date-parts', [['', '']])[0][0])
                
                online_year = str(message.get('journal-issue', {}).get('published-online', {}).get('date-parts', [['', '']])[0][0])

                issns = message.get('issn-type', [{}])

                print_issn = [standardizer.journal_issn(i.get('value', '')) for i in issns if i.get('type', '') == 'print']
                print_issn = [i for i in print_issn if i is not None]

                if len(print_issn) == 0:
                    print_issn = ''

                elif len(print_issn) == 1:
                    print_issn = print_issn[0]

                else:
                    logging.warning(f'Há múltiplos códigos ISSN {print_issn}')

                online_issn = [standardizer.journal_issn(i.get('value', '')) for i in issns if i.get('type', '') == 'electronic']
                online_issn = [o for o in online_issn if o is not None]

                if len(online_issn) == 0:
                    online_issn = ''

                elif len(online_issn) == 1:
                    online_issn = online_issn[0]

                else:
                    logging.warning(f'Há múltiplos códigos ISSN {online_issn}')

                for cit in cited_forms:
                    if print_issn != '' and cit != '' and print_year != '' and volume != '':
                        # in some cases the volume value is composed of two numbers separated by a hyphen
                        if '-' in volume:
                            volume = volume.split('-')[0]
                        metadata_print_str = '|'.join([print_issn, cit, print_year, volume, issue])
                        cited_forms_with_metadata.add(metadata_print_str)

                    if online_issn != '' and cit != '' and online_year != '' and volume != '':
                        metadata_online_str = '|'.join([online_issn, cit, online_year, volume, issue])
                        cited_forms_with_metadata.add(metadata_online_str)
        try:
            line = file_crossref.readline()
        except UnicodeDecodeError:
            logging.error(f'Erro de codificação em linha {line}')

    file_crossref.close()

    return cited_forms_with_metadata


def read_normalized_data(path_data):
    results = set()

    file_data = open(path_data)

    line = file_data.readline()

    while line:
        els = line.strip().split('|')

        std_issn = standardizer.journal_issn(els[0])
        if not std_issn:
            logging.warning(f'{els[0]} não é um ISSN')
        else:
            els = [std_issn] + els[1:]
            results.add('|'.join(els))

        line = file_data.readline()

    return results


def get_wos_si_source_data(path_wos_si_source, ignore_extra_cols=True):
    file_wos_si_source = open(path_wos_si_source)

    line = file_wos_si_source.readline()

    cited_forms_with_metadata = set()

    while line:
        els = line.split('|')

        if len(els) != 7:
            logging.warning(f'Linha {line} é inválida')
        else:
            issn = standardizer.journal_issn(els[0])

            cited_form_1 = standardizer.journal_title_for_deduplication(els[1].strip().lower()).upper()

            year = els[2]

            try:
                volume = str(standardizer.issue_volume(els[3]))
            except standardizer.ImpossibleConvertionToIntError:
                volume = els[3]

            if not ignore_extra_cols:
                cited_form_2 = standardizer.journal_title_for_deduplication(els[4].strip().lower()).upper()

            if issn != '' and year != '' and volume != '':
                if cited_form_1 != '':
                    metadata_str_1 = '|'.join([issn, cited_form_1, year, volume, ''])
                    cited_forms_with_metadata.add(metadata_str_1)

                if not ignore_extra_cols:
                    if cited_form_2 != '':
                        metadata_str_2 = '|'.join([issn, cited_form_2, year, volume, ''])
                        cited_forms_with_metadata.add(metadata_str_2)

        line = file_wos_si_source.readline()

    file_wos_si_source.close()

    return cited_forms_with_metadata


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input_dir',
        required=True,
        help='Diretório com fontes de dados pré-processadas'
    )

    parser.add_argument(
        '--output_file',
        required=True,
        help='Base de validação ano-volume processada'
    )

    parser.add_argument(
        '--loglevel',
        default=logging.INFO,
        help='Escopo de mensagens de log'
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    path_file_refs_wos_doi = os.path.join(args.input_dir, 'refs_wos_doi.txt')
    path_file_crossref_results = os.path.join(args.input_dir, 'crossref_results.json')
    path_file_wos_si_source = os.path.join(args.input_dir, 'WoS-refs_SIsource_ISSN.txt')
    path_file_scielo = os.path.join(args.input_dir, 'scielo_year_volume.tsv')
    path_file_locator_plus = os.path.join(args.input_dir, 'locator_plus.csv')
    path_file_proj56_raw = os.path.join(args.input_dir, 'proj_056_raw.csv')
    path_file_proj56_crossref = os.path.join(args.input_dir, 'proj_056_crossref.jsonl')

    logging.info(f'Lendo {path_file_refs_wos_doi}')
    doi2cited_form = get_doi2cited_form_dict(path_file_refs_wos_doi)

    logging.info(f'Lendo {path_file_crossref_results}')
    crossref_data = get_cited_forms_with_metadata(path_file_crossref_results, doi2cited_form)

    logging.info(f'Lendo {path_file_wos_si_source}')
    wos_data = get_wos_si_source_data(path_file_wos_si_source)

    logging.info(f'Lendo {path_file_scielo}')
    scielo_data = read_normalized_data(path_file_scielo)

    logging.info(f'Lendo {path_file_locator_plus}')
    locator_plus_data = read_normalized_data(path_file_locator_plus)

    logging.info(f'Lendo {path_file_proj56_raw}')
    proj56_data = load_proj56_raw(path_file_proj56_raw)

    logging.info(f'Lendo {path_file_proj56_crossref}')
    proj56_crossref_data = load_proj56_crossref(path_file_proj56_crossref)

    logging.info('Mesclando dados')
    d1 = crossref_data.union(wos_data)
    d2 = d1.union(scielo_data)
    d3 = d2.union(locator_plus_data)
    d4 = d3.union(proj56_data)
    d5 = d4.union(proj56_crossref_data)

    logging.info(f'Gravando base ano-volume em {args.output_file}')

    with open(args.output_file, 'w') as fout:
        for c in sorted(d5):
            fout.write(c + '\n')


if __name__ == '__main__':
    main()
