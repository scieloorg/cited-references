import argparse
import csv
import logging
import json
import os

from scielo_scholarly_data import standardizer


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


def load_crossref(path_crossref):
    year_vol_data = set()

    with open(path_crossref) as fin:
        for line in fin:
            json_line = json.loads(line)

            message = json_line.get('message', {})
            
            if isinstance(message, dict):
                try:
                    volume = str(standardizer.issue_volume(message.get('volume', '')))
                except (standardizer.ImpossibleConvertionToIntError, standardizer.InvalidRomanNumeralError):
                    volume = message.get('volume', '')

                if not volume:
                    continue

                year = str(message.get('issued', {}).get('date-parts', [['']])[0][0])
                if not year:
                    continue

                issns = sorted(set([standardizer.journal_issn(i.get('value', '')) for i in message.get('issn-type', [{}]) if i.get('type', '')]))
                issns = [i for i in issns if i is not None]

                titles = [standardizer.journal_title_for_deduplication(t.lower()).upper() for t in message.get('container-title', [])] 
                titles.extend([standardizer.journal_title_for_deduplication(t.lower()).upper() for t in message.get('short-container-title', [])])
                titles = [t for t in titles if len(t) > 0]
                

                for tit in titles:
                    for isn in issns:
                        yvd = '|'.join([
                            isn,
                            tit,
                            year,
                            volume,
                            '',
                        ])
                    year_vol_data.add(yvd)

    return year_vol_data


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

    path_file_crossref_results = os.path.join(args.input_dir, 'crossref_results.json')
    path_file_scielo = os.path.join(args.input_dir, 'scielo_year_volume.tsv')
    path_file_locator_plus = os.path.join(args.input_dir, 'locator_plus.csv')
    path_file_proj56_crossref = os.path.join(args.input_dir, 'proj_056_crossref.jsonl')

    logging.info(f'Lendo {path_file_scielo}')
    scielo_data = read_normalized_data(path_file_scielo)

    logging.info(f'Lendo {path_file_locator_plus}')
    locator_plus_data = read_normalized_data(path_file_locator_plus)

    logging.info(f'Lendo {path_file_proj56_crossref}')
    proj56_crossref_data = load_proj56_crossref(path_file_proj56_crossref)

    logging.info(f'Lendo {path_file_crossref_results}')
    crossref_data = load_crossref(path_file_crossref_results)

    logging.info('Mesclando dados')
    d1 = scielo_data.union(locator_plus_data)
    d2 = d1.union(proj56_crossref_data)
    d3 = d2.union(crossref_data)

    logging.info(f'Gravando base ano-volume em {args.output_file}')
    with open(args.output_file, 'w') as fout:
        for c in sorted(d3):
            fout.write(c + '\n')


if __name__ == '__main__':
    main()
