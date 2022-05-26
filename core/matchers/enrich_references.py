import argparse
import logging
import magic
import re
import os

from scielo_scholarly_data import standardizer
from core.util import file
from core.model.citation import Citation
from result_code import *


LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', logging.INFO)
MIN_WORD_LENGTH = int(os.environ.get('MIN_WORD_LENGTH', '2'))
MIN_TITLE_LENGTH = int(os.environ.get('MIN_TITLE_LENGTH', '6'))
MIN_WORDS_NUMBER = int(os.environ.get('MIN_WORDS_NUMBER', '2'))
MIN_COMPARABLE_WORDS_NUMBER = int(os.environ.get('MIN_COMPARABLE_WORDS_NUMBER', '2'))


def fuzzy_match(title: str, data: dict, standardize=False):
    words = title.split(' ')

    if standardize:
        cleaned_title = standardizer.journal_title_for_deduplication(title).upper()
    else:
        cleaned_title = title

    valid_words = [w for w in words if len(w) >= MIN_WORD_LENGTH]

    if len(cleaned_title) >= MIN_TITLE_LENGTH and len(words) >= MIN_WORDS_NUMBER and len(valid_words) >= MIN_COMPARABLE_WORDS_NUMBER:
        pattern = r'[\w|\s]*'.join([word for word in words]) + '[\w|\s]*'

        title_pattern = re.compile(pattern, re.UNICODE)

        fuzzy_matches = []

        for oficial_title in [ot for ot in data.keys() if ot.startswith(words[0])]:
            match = title_pattern.fullmatch(oficial_title)

            if match:
                fuzzy_matches.extend(data[oficial_title].split('#'))

        return set(fuzzy_matches)

    return set()


def infer_volume(issn: str, year: int, data: dict):
    if issn in data:
        a, b = data[issn]
        return round(a + (b * year))


def get_issns_list_sign(issns_list):
    return '#'.join(sorted(issns_list))


def clean_previous_results(citation):
    for pv in ['result', 'result_code', 'cited_issnl']:
        try:
            del citation.__dict__[pv]
        except KeyError:
            ...


def extract_essential_data(citation):
    try:                    
        cited_journal_title_cleaned = standardizer.journal_title_for_deduplication(citation.cited_journal).upper()
    except AttributeError:
        cited_journal_title_cleaned = ''

    if not cited_journal_title_cleaned:
        try:
            cited_journal_title_cleaned = standardizer.journal_title_for_deduplication(citation.cited_source).upper()
        except AttributeError:
            cited_journal_title_cleaned = ''

    try:
        cited_year_cleaned = str(standardizer.document_publication_date(citation.cited_year, only_year=True))
    except Exception:
        cited_year_cleaned = ''

    try:
        cited_volume_cleaned = standardizer.issue_volume(citation.cited_vol)
    except Exception:
        cited_volume_cleaned = ''

    return cited_journal_title_cleaned, cited_year_cleaned, cited_volume_cleaned


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--title_to_issnl',
        required=True,
        help='Base de correção Título de periódico -> ISSN-L',
    )

    parser.add_argument(
        '--issnl_to_all',
        required=True,
        help='Base de correção ISSNL -> Metadados',
    )

    parser.add_argument(
        '--title_year_volume_to_issn',
        required=True,
        help='Base de correção Título de periódico, Ano, Volume -> ISSN',
    )

    parser.add_argument(
        '--artifitial_title_year_volume_to_issn',
        required=True,
        help='Base de correção artificial Título de periódico, Ano, Volume -> ISSN'
    )

    parser.add_argument(
        '--equations',
        help='Base de correção ISSN -> Regressão Linear',
    )

    parser.add_argument(
        '--use_fuzzy',
        action='store_true',
        help='Usar deduplicação aproximada de Título de periódico',
    )

    parser.add_argument(
        '--output',
        default='results.jsonl',
        help='Arquivo de resultados, isto é, com referências citadas enriquecidas'
    )

    parser.add_argument(
        '--input',
        required=True,
        help='Arquivo de entrada, isto é, com referências citadas a serem enriquecidas'
    )

    parser.add_argument(
        '--input_format',
        default='json',
        choices=['csv', 'json'],
        help='Formato de arquivo de entrada'
    )

    parser.add_argument(
        '--ignore_previous_result',
        default=False,
        action='store_true',
        help='Indica para ignorar cited_issnl pré-existente'
    )

    params = parser.parse_args()

    print('Carregando base Title to ISSN-L...')
    title2issnl = file.load_title_to_issnl(params.title_to_issnl)

    print('Carregando base ISSN-L to All...')
    issn2issnl, issn2titles = file.load_issnl_to_all(params.issnl_to_all)

    print('Carregando base Title Year Volume to ISSN...')
    title_year_volume2issn = file.load_year_volume(params.title_year_volume_to_issn, issn2issnl)

    print('Carregando base artificial Title Year Volume to ISSN...')
    artifitial_title_year_volume2issn = file.load_year_volume(params.artifitial_title_year_volume_to_issn, issn2issnl)

    print('Carregando regressões lineares')
    issn2equations = file.load_equations(params.equations)

    with open(params.output, 'w') as fout:
        print('Deduplicando...')
        line_counter = 0

        with open(params.input, encoding='iso-8859-1') as fin:
            line = fin.readline()
            while line:
                line_counter += 1
                if line_counter % 100 == 0:
                    print(line_counter)
                    fout.flush()
                cit = Citation(line, format=params.input_format)

                # Caso citação já tenha sido tratada e ISSN-L é válido
                if not params.ignore_previous_result and 'cited_issnl' in cit.__dict__:
                    fout.write(cit.to_json() + '\n')
                else:
                    clean_previous_results(cit)
                    cited_journal_title_cleaned, cited_year_cleaned, cited_volume_cleaned = extract_essential_data(cit)

                    # Caso haja título de periódico
                    if cited_journal_title_cleaned:

                        # Caso ocorra correspondência exata entre título de periódico e base de correção
                        if cited_journal_title_cleaned in title2issnl:
                            exact_match_issnls = title2issnl.get(cited_journal_title_cleaned, '').split('#')
                            exact_match_issnls_std = [standardizer.journal_issn(i) for i in exact_match_issnls]
                            cit.setattr('exact_match_issnls_size', len(exact_match_issnls))

                            # Correspondência exata com apenas um ISSN-L
                            if cit.exact_match_issnls_size == 1:
                                standardized_issn = exact_match_issnls_std[0]
                                cit.setattr('cited_issnl', standardized_issn)
                                cit.setattr('result_code', SUCCESS_EXACT_MATCH)
                                fout.write(cit.to_json() + '\n')

                            # Correspondência com mais de um ISSN-L
                            else:
                                # Caso haja ano
                                if cited_year_cleaned.isdigit():
                                    validated = False

                                    # Caso haja volume
                                    if cited_volume_cleaned != '':
                                        title_year_volume_key = '-'.join([cited_journal_title_cleaned, cited_year_cleaned, cited_volume_cleaned])

                                        if title_year_volume_key in title_year_volume2issn:
                                            yvk_issns = list(title_year_volume2issn[title_year_volume_key])

                                            # Desambiguou com base secundária
                                            if len(yvk_issns) == 1:
                                                yvk_standardized_issn = standardizer.journal_issn(yvk_issns[0])

                                                # ISSN indicado é um daqueles com dúvida
                                                if yvk_standardized_issn in exact_match_issnls_std:
                                                    cit.setattr('cited_issnl', yvk_standardized_issn)
                                                    cit.setattr('result_code', SUCCESS_EXACT_MATCH_YEAR_VOL)
                                                    cit.setattr('title_year_volume_key', title_year_volume_key)
                                                    fout.write(cit.to_json() + '\n')
                                                    validated = True

                                    # Não validou, tenta usar volumes inferidos com base ano-volume
                                    result_issn_to_data = {'issns': set(), 'tyv_keys': {}}
                                    stop_trying = False

                                    if not validated:
                                        for i in exact_match_issnls:
                                            if not stop_trying:
                                                # infere volume
                                                cit_volume_inferred = infer_volume(i, int(cited_year_cleaned), issn2equations)

                                                if cit_volume_inferred:
                                                    # procura por chaves na base secundária
                                                    for voli in [vi for vi in range(cit_volume_inferred - 1, cit_volume_inferred + 2) if vi > 0]:
                                                        tyv_key = '-'.join([
                                                            cited_journal_title_cleaned, 
                                                            cited_year_cleaned, 
                                                            str(voli)
                                                        ])
                                                        
                                                        possible_issnls = title_year_volume2issn.get(tyv_key, set())
                                                        if len(possible_issnls) > 0:
                                                            result_issn_to_data['issns'] = result_issn_to_data['issns'].union(possible_issnls)
                                                            
                                                            possible_issnls_sign = get_issns_list_sign(possible_issnls)
                                                            if possible_issnls_sign not in result_issn_to_data['tyv_keys']:
                                                                result_issn_to_data['tyv_keys'][possible_issnls_sign] = set()

                                                            result_issn_to_data['tyv_keys'][possible_issnls_sign].add(tyv_key)

                                                        if len(result_issn_to_data['issns']) > 1:
                                                            stop_trying = True
                                                            cit.setattr('result_code', ERROR_EXACT_MATCH_YEAR_VOL_INF)
                                                            fout.write(cit.to_json() + '\n')
                                                            break

                                        if len(result_issn_to_data['issns']) == 1:
                                            matched_issn = list(result_issn_to_data['issns'])[0]
                                            matched_issn_std = standardizer.journal_issn(matched_issn)
                                            matched_tyv_key = list(result_issn_to_data['tyv_keys'][matched_issn])[0]

                                            # ISSN indicado é um daqueles com dúvida
                                            if matched_issn_std in exact_match_issnls_std:
                                                cit.setattr('cited_issnl', matched_issn_std)
                                                cit.setattr('result_code', SUCCESS_EXACT_MATCH_YEAR_VOL_INF)
                                                cit.setattr('title_year_volume_key', matched_tyv_key)
                                                fout.write(cit.to_json() + '\n')
                                                validated = True

                                    # Não validou, tenta usar base ano-volume artificial
                                    if not validated and not stop_trying: 
                                        if cited_volume_cleaned:
                                            key_title_year_volume = '-'.join([cited_journal_title_cleaned, cited_year_cleaned, cited_volume_cleaned])
                                            ktyv_values = artifitial_title_year_volume2issn.get(key_title_year_volume, set())
                                            ktyv_values = ktyv_values.union(title_year_volume2issn.get(key_title_year_volume, set()))
                                            
                                            if len(ktyv_values) == 1:
                                                ktyv_standardized_issn = standardizer.journal_issn(list(ktyv_values)[0])

                                                if ktyv_standardized_issn in exact_match_issnls_std:
                                                    cit.setattr('cited_issnl', ktyv_standardized_issn)
                                                    cit.setattr('result_code', SUCCESS_EXACT_MATCH_YEAR_VOL_ART)
                                                    cit.setattr('title_year_volume_key', key_title_year_volume)
                                                    fout.write(cit.to_json() + '\n')
                                                    validated = True

                                    # Não validou, tenta usar volume inferido com base ano-volume artificial
                                    if not validated and not stop_trying:
                                        for i in exact_match_issnls:
                                            if not stop_trying:
                                                # infere volume
                                                cit_volume_inferred = infer_volume(i, int(cited_year_cleaned), issn2equations)

                                                if cit_volume_inferred:
                                                    # procura por chaves na base secundária
                                                    for voli in [vi for vi in range(cit_volume_inferred - 1, cit_volume_inferred + 2) if vi > 0]:
                                                        tyv_key = '-'.join([
                                                            cited_journal_title_cleaned, 
                                                            cited_year_cleaned, 
                                                            str(voli)
                                                        ])
                                                        
                                                        possible_issnls = artifitial_title_year_volume2issn.get(tyv_key, set())
                                                        if len(possible_issnls) > 0:
                                                            result_issn_to_data['issns'] = result_issn_to_data['issns'].union(possible_issnls)
                                                            
                                                            possible_issnls_sign = get_issns_list_sign(possible_issnls)
                                                            if possible_issnls_sign not in result_issn_to_data['tyv_keys']:
                                                                result_issn_to_data['tyv_keys'][possible_issnls_sign] = set()

                                                            result_issn_to_data['tyv_keys'][possible_issnls_sign].add(tyv_key)

                                                        if len(result_issn_to_data['issns']) > 1:
                                                            stop_trying = True
                                                            cit.setattr('result_code', ERROR_EXACT_MATCH_YEAR_VOL_INF_ART)
                                                            fout.write(cit.to_json() + '\n')
                                                            break

                                        if len(result_issn_to_data['issns']) == 1:
                                            matched_issn = list(result_issn_to_data['issns'])[0]
                                            matched_issn_std = standardizer.journal_issn(matched_issn)
                                            matched_tyv_key = list(result_issn_to_data['tyv_keys'][matched_issn])[0]

                                            # ISSN indicado é um daqueles com dúvida
                                            if matched_issn_std in exact_match_issnls_std:
                                                cit.setattr('cited_issnl', matched_issn_std)
                                                cit.setattr('result_code', SUCCESS_EXACT_MATCH_YEAR_VOL_INF_ART)
                                                cit.setattr('title_year_volume_key', matched_tyv_key)
                                                fout.write(cit.to_json() + '\n')
                                                validated = True

                                    # Não validou
                                    if not validated and not stop_trying:
                                        cit.setattr('result_code', ERROR_EXACT_MATCH_UNDECIDABLE)
                                        fout.write(cit.to_json() + '\n')

                                # Não há dados de ano para fazer desambiguação
                                else:
                                    cit.setattr('result_code', ERROR_EXACT_MATCH_INVALID_YEAR)
                                    fout.write(cit.to_json() + '\n')

                        # Correspondência inexata
                        else:
                            if params.use_fuzzy:
                                # Não há dados suficientes para validar uma possível correspondência inexata
                                if not cited_year_cleaned.isdigit():
                                    cit.setattr('result_code', ERROR_FUZZY_MATCH_INVALID_YEAR)
                                    fout.write(cit.to_json() + '\n')

                                else:
                                    fuzzy_match_issnls = fuzzy_match(cited_journal_title_cleaned, title2issnl)

                                    if len(fuzzy_match_issnls) > 0:
                                        cit.setattr('fuzzy_match_issnls_size', len(fuzzy_match_issnls))

                                        fz_validated = False
                                        fz_stop_trying = False

                                        # Há volume
                                        if cited_volume_cleaned != '':

                                            # Houve correspondência inexata
                                            if len(fuzzy_match_issnls) > 0:
                                                fz_possible_titles = set()
                                                valid_fuzzy_match_issnls = set()

                                                for i in fuzzy_match_issnls:
                                                    fz_possible_titles = fz_possible_titles.union(set(issn2titles.get(i, set())))

                                                for fz_title in fz_possible_titles:
                                                    fz_title_year_volume_key = '-'.join([fz_title, cited_year_cleaned, cited_volume_cleaned])

                                                    if fz_title_year_volume_key in title_year_volume2issn:
                                                        valid_fuzzy_match_issnls = valid_fuzzy_match_issnls.union(title_year_volume2issn[fz_title_year_volume_key])

                                                        if len(valid_fuzzy_match_issnls) > 1:
                                                            cit.setattr('result_code', ERROR_FUZZY_MATCH_YEAR_VOL)
                                                            fout.write(cit.to_json() + '\n')
                                                            fz_stop_trying = True
                                                            break

                                                # Validou e desambiguou com base secundária
                                                if len(valid_fuzzy_match_issnls) == 1:
                                                    fz_standardized_issn = standardizer.journal_issn(list(valid_fuzzy_match_issnls)[0])

                                                    # ISSN indicado é um daqueles com dúvida
                                                    if fz_standardized_issn in fuzzy_match_issnls:
                                                        cit.setattr('cited_issnl', fz_standardized_issn)
                                                        cit.setattr('result_code', SUCCESS_FUZZY_MATCH_YEAR_VOL)
                                                        cit.setattr('title_year_volume_key', matched_tyv_key)
                                                        fout.write(cit.to_json() + '\n')
                                                        fz_validated = True

                                        # Não validou, tenta usar volumes inferidos
                                        fz_result_issn_to_data = {'issns': set(), 'tyv_keys': {}}
                                        if not fz_validated and not fz_stop_trying:
                                            for i in fuzzy_match_issnls:
                                                if not fz_stop_trying:
                                                    fz_cit_volume_inferred = infer_volume(i, int(cited_year_cleaned), issn2equations)

                                                    if fz_cit_volume_inferred:
                                                        for voli in [vi for vi in range(fz_cit_volume_inferred - 1, fz_cit_volume_inferred + 2) if vi > 0]:
                                                            fz_tyv_key = '-'.join([
                                                                cited_journal_title_cleaned,
                                                                cited_year_cleaned,
                                                                str(voli),
                                                            ])
                                                            
                                                            fz_possible_issnls = title_year_volume2issn.get(fz_tyv_key, set())
                                                            if len(fz_possible_issnls) > 0:
                                                                fz_result_issn_to_data['issns'] = fz_result_issn_to_data['issns'].union(fz_possible_issnls)

                                                                fz_possible_issnls_sign = get_issns_list_sign(fz_possible_issnls)
                                                                if fz_possible_issnls_sign not in fz_result_issn_to_data['tyv_keys']:
                                                                    fz_result_issn_to_data['tyv_keys'][fz_possible_issnls_sign] = set()

                                                                fz_result_issn_to_data['tyv_keys'][fz_possible_issnls_sign].add(fz_tyv_key)

                                                            if len(fz_result_issn_to_data['issns']) > 1:
                                                                cit.setattr('result_code', ERROR_FUZZY_MATCH_YEAR_VOL_INF)
                                                                fout.write(cit.to_json() + '\n')
                                                                fz_stop_trying = True
                                                                break

                                            if len(fz_result_issn_to_data['issns']) == 1:
                                                fz_matched_issn = list(fz_result_issn_to_data['issns'])[0]
                                                fz_matched_issn_std = standardizer.journal_issn(fz_matched_issn)
                                                fz_matched_tyv_key = list(fz_result_issn_to_data['tyv_keys'][fz_matched_issn])[0]

                                                if fz_matched_issn in fuzzy_match_issnls:                                                    
                                                    cit.setattr('cited_issnl', fz_matched_issn_std)
                                                    cit.setattr('result_code', SUCCESS_FUZZY_MATCH_YEAR_VOL_INF)
                                                    cit.setattr('title_year_volume_key', fz_matched_tyv_key)
                                                    fout.write(cit.to_json() + '\n')
                                                    fz_validated = True

                                        # Não validou, tenta usar base artificial de ano-volume
                                        if not fz_validated and not fz_stop_trying: 
                                            if cited_volume_cleaned:
                                                key_fz_title_year_volume = '-'.join([cited_journal_title_cleaned, cited_year_cleaned, cited_volume_cleaned])
                                                kftyv_values = artifitial_title_year_volume2issn.get(key_fz_title_year_volume, set())
                                                kftyv_values = kftyv_values.union(title_year_volume2issn.get(key_fz_title_year_volume, set()))

                                                if len(kftyv_values) == 1:
                                                    standardized_issn = standardizer.journal_issn(list(kftyv_values)[0])

                                                    if standardized_issn in fuzzy_match_issnls:
                                                        cit.setattr('cited_issnl', standardized_issn)
                                                        cit.setattr('result_code', SUCCESS_FUZZY_MATCH_YEAR_VOL_ART)
                                                        fout.write(cit.to_json() + '\n')
                                                        validated = True

                                        # Não validou, tenta usar volume inferido com base artificial ano-volume
                                        if not fz_validated and not fz_stop_trying:
                                            for i in fuzzy_match_issnls:
                                                if not fz_stop_trying:
                                                    fz_cit_volume_inferred = infer_volume(i, int(cited_year_cleaned), issn2equations)

                                                    if fz_cit_volume_inferred:
                                                        for voli in [vi for vi in range(fz_cit_volume_inferred - 1, fz_cit_volume_inferred + 2) if vi > 0]:
                                                            fz_tyv_key = '-'.join([
                                                                cited_journal_title_cleaned,
                                                                cited_year_cleaned,
                                                                str(voli),
                                                            ])
                                                            
                                                            fz_possible_issnls = artifitial_title_year_volume2issn.get(fz_tyv_key, set())
                                                            if len(fz_possible_issnls) > 0:
                                                                fz_result_issn_to_data['issns'] = fz_result_issn_to_data['issns'].union(fz_possible_issnls)

                                                                fz_possible_issnls_sign = get_issns_list_sign(fz_possible_issnls)
                                                                if fz_possible_issnls_sign not in fz_result_issn_to_data['tyv_keys']:
                                                                    fz_result_issn_to_data['tyv_keys'][fz_possible_issnls_sign] = set()

                                                                fz_result_issn_to_data['tyv_keys'][fz_possible_issnls_sign].add(fz_tyv_key)

                                                            if len(fz_result_issn_to_data['issns']) > 1:
                                                                cit.setattr('result_code', ERROR_FUZZY_MATCH_YEAR_VOL_INF_ART)
                                                                fout.write(cit.to_json() + '\n')
                                                                fz_stop_trying = True
                                                                break

                                            if len(fz_result_issn_to_data['issns']) == 1:
                                                fz_matched_issn = list(fz_result_issn_to_data['issns'])[0]
                                                fz_matched_issn_std = standardizer.journal_issn(fz_matched_issn)
                                                fz_matched_tyv_key = list(fz_result_issn_to_data['tyv_keys'][fz_matched_issn])[0]

                                                if fz_matched_issn in fuzzy_match_issnls:                                                    
                                                    cit.setattr('cited_issnl', fz_matched_issn_std)
                                                    cit.setattr('result_code', SUCCESS_FUZZY_MATCH_YEAR_VOL_INF_ART)
                                                    cit.setattr('title_year_volume_key', fz_matched_tyv_key)
                                                    fout.write(cit.to_json() + '\n')
                                                    fz_validated = True

                                        # Não validou
                                        if not fz_validated and not fz_stop_trying:
                                            cit.setattr('result_code', ERROR_FUZZY_MATCH_UNDECIDABLE)
                                            fout.write(cit.to_json() + '\n')

                                    # Não houve correspondência inexata
                                    else:
                                        cit.setattr('result_code', ERROR_JOURNAL_TITLE_NOT_FOUND)
                                        fout.write(cit.to_json() + '\n')

                            # Não tentou fazer correspondência
                            else:
                                cit.setattr('result_code', NOT_CONDUCTED_MATCH_FORCED_BY_USER)
                                fout.write(cit.to_json() + '\n')

                    # Caso não haja título de periódico
                    else:
                        # Mas há DOI
                        if hasattr(cit, 'cited_doi') and cit.cited_doi:
                            cit.setattr('result_code', NOT_CONDUCTED_MATCH_DOI_EXISTS)
                            fout.write(cit.to_json() + '\n')

                        # E não há DOI
                        else:
                            cit.setattr('result_code', ERROR_JOURNAL_TITLE_IS_EMPTY)
                            fout.write(cit.to_json() + '\n')

                line = fin.readline()


if __name__ == '__main__':
    main()
