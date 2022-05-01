import argparse
import csv
import re

from scielo_scholarly_data import standardizer
from core.util import file
from core.model.citation import Citation


def fuzzy_match(title: str, data: dict, standardize=False):
    words = title.split(' ')

    if standardize:
        cleaned_title = standardizer.journal_title_for_deduplication(title).upper()
    else:
        cleaned_title = title

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


def infer_volume(issn: str, year: int, data: dict):
    if issn in data:
        a, b = data[issn]
        return round(a + (b * year))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--title_to_issnl',
        required=True,
    )

    parser.add_argument(
        '--issnl_to_all',
        required=True,
    )

    parser.add_argument(
        '--title_year_volume_to_issn',
        required=True,
    )

    parser.add_argument(
        '--equations',
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

    print('Carregando base Title to ISSN-L...')
    title2issnl = file.load_title_to_issnl(params.title_to_issnl)

    print('Carregando base ISSN-L to All...')
    issn2issnl, issn2titles = file.load_issnl_to_all(params.issnl_to_all)

    print('Carregando base Title Year Volume to ISSN...')
    title_year_volume2issn = file.load_year_volume(params.title_year_volume_to_issn, issn2issnl)

    print('Carregando regressões lineares')
    issn2equations = file.load_equations(params.equations)

    results_dir = file.generate_folder_name(params.output)
    file.check_dir(results_dir)
    d_results = file.open_files(results_dir)

    d_results['insufficient-data'].write(Citation({}).header_str() + '\n')
    d_results['pre-existing-issn'].write(Citation({}).header_str() + '\n')
    d_results['exact-match'].write(Citation({}).header_str(extra_headers=['exact_match_issnl']) + '\n')
    d_results['exact-match-homonymous-fixed'].write(Citation({}).header_str(extra_headers=['exact_match_issnls', 'exact_match_issnl_homonymous_fixed']) + '\n')
    d_results['exact-match-homonymous-fixed-volume-inferred'].write(Citation({}).header_str(extra_headers=['exact_match_issnls', 'exact_match_issnl_homonymous_fixed', 'volume_inferred']) + '\n')
    d_results['exact-match-homonymous-not-fixed'].write(Citation({}).header_str(extra_headers=['exact_match_issnls', 'exact_match_issnls_size']) + '\n')
    d_results['exact-match-homonymous-insufficient-data'].write(Citation({}).header_str(extra_headers=['exact_match_issnls', 'exact_match_issnls_size']) + '\n')
    d_results['fuzzy-match-insufficient-data'].write(Citation({}).header_str() + '\n')
    d_results['fuzzy-match-validated'].write(Citation({}).header_str(extra_headers=['fuzzy_match_issnls', 'fuzzy_match_issnl_validated']) + '\n')
    d_results['fuzzy-match-validated-volume-inferred'].write(Citation({}).header_str(extra_headers=['fuzzy_match_issnls', 'fuzzy_match_issnl_validated', 'volume_inferred']) + '\n')
    d_results['fuzzy-match-not-validated'].write(Citation({}).header_str(extra_headers=['fuzzy_match_issnls', 'fuzzy_match_issnls_size']) + '\n')
    d_results['fuzzy-todo'].write(Citation({}).header_str() + '\n')
    d_results['unmatch'].write(Citation({}).header_str() + '\n')

    print('Deduplicando...')
    pid_counter = 0
    with open(params.input) as fin:
        csv_reader = csv.DictReader(fin, delimiter=',')

        for row in csv_reader:
            pid_counter += 1
            if pid_counter % 1000 == 0:
                print(pid_counter)

            cit = Citation(row)
            if not hasattr(cit, 'line_id'):
                cit.line_id = str(pid_counter)

            cit_journal_title_cleaned = standardizer.journal_title_for_deduplication(cit.ref_sourcetitle_set).upper()
            cit_year_cleaned = str(standardizer.document_publication_date(cit.ref_pubyear, only_year=True))
            cit_volume_cleaned = standardizer.issue_volume(cit.ref_volume)

            # Caso já exista ISSN preenchido
            if cit.ref_issn_print != '' or cit.ref_issn_electronic != '':
                d_results['pre-existing-issn'].write(cit.attrs_str() + '\n')

            # Caso haja título
            elif cit_journal_title_cleaned:

                # Correspondência exata
                if cit_journal_title_cleaned in title2issnl:
                    exact_match_issnls = title2issnl.get(cit_journal_title_cleaned, '').split('#')
                    cit.setattr('exact_match_issnls', exact_match_issnls)
                    cit.setattr('exact_match_issnls_size', len(exact_match_issnls))

                    # Apenas 1 ISSN-L
                    if cit.exact_match_issnls_size == 1:
                        standardized_issn = standardizer.journal_issn(exact_match_issnls[0])
                        cit.setattr('exact_match_issnl', standardized_issn)
                        d_results['exact-match'].write(cit.attrs_str(extra_attrs=['exact_match_issnl']) + '\n')

                    # Mais de 1 ISSN-L
                    else:
                        standardized_issnls = [standardizer.journal_issn(i) for i in exact_match_issnls]
                        cit.setattr('exact_match_issnls', '#'.join(standardized_issnls))

                        # Caso haja ano
                        if cit_year_cleaned.isdigit():
                            validated = False

                            # Caso haja volume
                            if cit_volume_cleaned != '':
                                title_year_volume_key = '-'.join([cit_journal_title_cleaned, cit_year_cleaned, cit_volume_cleaned])

                                if title_year_volume_key in title_year_volume2issn:
                                    yvk_issns = list(title_year_volume2issn[title_year_volume_key])

                                    # Desambiguou com base secundária
                                    if len(yvk_issns) == 1:
                                        standardized_issn = standardizer.journal_issn(yvk_issns[0])
                                        cit.setattr('exact_match_issnl_homonymous_fixed', standardized_issn)
                                        d_results['exact-match-homonymous-fixed'].write(cit.attrs_str(extra_attrs=['exact_match_issnls', 'exact_match_issnl_homonymous_fixed']) + '\n')
                                        validated = True

                            # Não validou, tenta usar volumes inferidos
                            if not validated:
                                title_year_volume_infered = set()

                                for i in exact_match_issnls:
                                    cit_volume_inferred = infer_volume(i, int(cit_year_cleaned), issn2equations)
                                    if cit_volume_inferred:
                                        vols = [vi for vi in range(cit_volume_inferred - 1, cit_volume_inferred + 2) if vi > 0]
                                        for voli in vols:
                                            title_year_volume_infered.add('-'.join([cit_journal_title_cleaned, cit_year_cleaned, str(voli)]))

                                        cit.setattr('volume_inferred', str('#'.join([str(v) for v in vols])))

                                inferred_yvk_issns = set()
                                for tyvi in title_year_volume_infered:
                                    if tyvi in title_year_volume2issn:
                                        inferred_yvk_issns = inferred_yvk_issns.union(title_year_volume2issn[tyvi])

                                if len(inferred_yvk_issns) == 1:
                                    standardized_issn = standardizer.journal_issn(list(inferred_yvk_issns)[0])
                                    cit.setattr('exact_match_issnl_homonymous_fixed', standardized_issn)
                                    d_results['exact-match-homonymous-fixed-volume-inferred'].write(cit.attrs_str(extra_attrs=['exact_match_issnls', 'exact_match_issnl_homonymous_fixed', 'volume_inferred']) + '\n')

                                # Não houve desambiguação
                                else:
                                    d_results['exact-match-homonymous-not-fixed'].write(cit.attrs_str(extra_attrs=['exact_match_issnls', 'exact_match_issnls_size']) + '\n')

                        # Não há dados de ano para fazer desambiguação
                        else:
                            d_results['exact-match-homonymous-insufficient-data'].write(cit.attrs_str(extra_attrs=['exact_match_issnls', 'exact_match_issnls_size']) + '\n')

                # Correspondência inexata
                else:
                    if params.use_fuzzy:
                        # Não há dados suficientes para validar uma possível correspondência inexata
                        if not cit_year_cleaned.isdigit():
                            d_results['fuzzy-match-insufficient-data'].write(cit.attrs_str() + '\n')

                        else:
                            fuzzy_match_issnls = fuzzy_match(cit_journal_title_cleaned, title2issnl)

                            if len(fuzzy_match_issnls) > 0:
                                fz_standardized_issnls = [standardizer.journal_issn(i) for i in fuzzy_match_issnls]
                                cit.setattr('fuzzy_match_issnls', '#'.join(fz_standardized_issnls))
                                cit.setattr('fuzzy_match_issnls_size', len(fuzzy_match_issnls))

                                fz_validated = False

                                # Há volume
                                if cit_volume_cleaned != '':

                                    # Houve correspondência inexata
                                    if len(fuzzy_match_issnls) > 0:
                                        valid_fuzzy_match_issnls = []

                                        # Coleta dados para validar com base secundária
                                        fz_possible_titles = set()
                                        valid_fuzzy_match_issnls = set()
                                        for i in fuzzy_match_issnls:
                                            fz_possible_titles = fz_possible_titles.union(set(issn2titles.get(i, set())))

                                        for fz_title in fz_possible_titles:
                                            fz_title_year_volume_key = '-'.join([fz_title, cit_year_cleaned, cit_volume_cleaned])

                                            if fz_title_year_volume_key in title_year_volume2issn:
                                                valid_fuzzy_match_issnls = valid_fuzzy_match_issnls.union(title_year_volume2issn[fz_title_year_volume_key])

                                        # Validou e desambiguou com base secundária
                                        if len(valid_fuzzy_match_issnls) == 1:
                                            fz_standardized_issn = standardizer.journal_issn(list(valid_fuzzy_match_issnls)[0])
                                            cit.setattr('fuzzy_match_issnl_validated', fz_standardized_issn)
                                            d_results['fuzzy-match-validated'].write(cit.attrs_str(extra_attrs=['fuzzy_match_issnls', 'fuzzy_match_issnl_validated']) + '\n')
                                            fz_validated = True

                                # Não validou, tenta usar volumes inferidos
                                if not fz_validated:
                                    title_year_volume_infered = set()

                                    for i in fuzzy_match_issnls:
                                        cit_volume_inferred = infer_volume(i, int(cit_year_cleaned), issn2equations)
                                        if cit_volume_inferred is not None:
                                            vols = [vi for vi in range(1 - cit_volume_inferred, cit_volume_inferred + 2) if vi > 0]
                                            for voli in vols:
                                                title_year_volume_infered.add('-'.join([cit_journal_title_cleaned, cit_year_cleaned, str(voli)]))

                                            cit.setattr('volume_inferred', str('#'.join([str(v) for v in vols])))

                                    fz_inferred_yvk_issns = set()
                                    for tyvi in title_year_volume_infered:
                                        if tyvi in title_year_volume2issn:
                                            fz_inferred_yvk_issns = fz_inferred_yvk_issns.union(title_year_volume2issn[tyvi])

                                    if len(fz_inferred_yvk_issns) == 1:
                                        standardized_issn = standardizer.journal_issn(list(fz_inferred_yvk_issns)[0])
                                        cit.setattr('fuzzy_match_issnl_homonymous_fixed', standardized_issn)
                                        d_results['fuzzy-match-validated-volume-inferred'].write(cit.attrs_str(extra_attrs=['fuzzy_match_issnls', 'fuzzy_match_issnl_homonymous_fixed', 'volume_inferred']) + '\n')

                                    # Não houve validação
                                    else:
                                        d_results['fuzzy-match-not-validated'].write(cit.attrs_str(extra_attrs=['fuzzy_match_issnls', 'fuzzy_match_issnls_size']) + '\n')

                            # Não houve correspondência inexata
                            else:
                                d_results['unmatch'].write(cit.attrs_str() + '\n')

                    # Não tentou fazer correspondência
                    else:
                        d_results['fuzzy-todo'].write(cit.attrs_str() + '\n')

            # Caso não haja dados suficientes
            else:
                # Caso haja valores DOI
                if cit.cited_doiset:
                    d_results['doiset'].write(cit.attrs_str() + '\n')

                # Caso não haja dados
                else:
                    d_results['insufficient-data'].write(cit.attrs_str() + '\n')

    file.close_files(d_results)


if __name__ == '__main__':
    main()
