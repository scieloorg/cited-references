#!/usr/bin/env python3
import json
import sys

USEFUL_COLUMNS_INDEXES = [1, 4, 13, 22, 25, 35, 37, 39, 56, 57, 81, 83, 96, 102]
USEFUL_KEYS = ['issn_l', '_issn', 'key_title', 'abbreviated_key_title', 'title_proper', 'country', 'dates_of_publication']
NORMALIZED_KEYS = ['issn_l', '_issn', 'key_title', 'abbreviated_key_title1', 'abbreviated_key_title2', 'abbreviated_key_title3', 'title_proper1', 'title_proper2', 'title_proper3', 'country', 'start_year', 'end_year']
IGNORE_ROW_PHRASES = []
IGNORE_ROW_PHRASES.append('Your search returned no results')
IGNORE_ROW_PHRASES.append('This legacy record is lacking')
IGNORE_ROW_PHRASES.append('This record corresponds to an ISSN that has been cancelled')


def clean_csv(path_csv: str):
    """
    Reads, cleans and filters the csv file provided by the issn-scrapper.
    :param path_csv: path of the portal issn's csv file obtained by the issn-scrapper script
    :return: a list of cleaned and filtered rows (invalid rows are discarded, useless columns are discarded)
    """
    tmp_cleaned_data = []
    len_ignored_lines = 0
    len_included_lines = 0
    try:
        file_tmp_original_data = open(path_csv)
        line = file_tmp_original_data.readline()
        while line:
            if IGNORE_ROW_PHRASES[0] not in line and IGNORE_ROW_PHRASES[1] not in line and IGNORE_ROW_PHRASES[2] not in line:
                line_json = json.loads(line)
                extracted_values = [line_json.get(k, []) for k in USEFUL_KEYS]
                normalized_values = []

                # appends the issnl
                issn_l = extracted_values[0]
                if len(issn_l) == 1:
                    normalized_values.append(issn_l[0])
                elif len(issn_l) > 1:
                    print('there is something wrong %s' % str(issn_l))
                elif len(issn_l) == 0:
                    normalized_values.append('')

                # append the issn
                issn = extracted_values[1]
                if issn != '':
                    normalized_values.append(issn)
                else:
                    normalized_values.append('')

                # appends the key title
                key_title = extracted_values[2]
                if len(key_title) == 1:
                    normalized_values.append(key_title[0])
                elif len(key_title) > 1:
                    print('there is something wrong %s' % str(key_title))
                else:
                    normalized_values.append('')

                # appends the abrev titles; there are a maximum of 3 abreviated titles - index 3
                abrev_titles = extracted_values[3]
                while len(abrev_titles) != 3:
                    abrev_titles.append('')
                normalized_values.extend(abrev_titles)

                # appends the titles proper; there are a maximum of 3 titles proper - index 4
                title_proper = extracted_values[4]
                while len(title_proper) != 3:
                    title_proper.append('')
                normalized_values.extend(title_proper)

                # appends the country
                country = extracted_values[5]
                if len(country) == 1:
                    normalized_values.append(country[0])
                elif len(country) > 1:
                    print('there is something wrong %s' % str(country))
                else:
                    normalized_values.append('')

                # appends the start and end years
                years = extracted_values[6]
                start_years = ''
                end_years = ''
                if len(years) > 0:
                    start_years = '#'.join(sorted([y[0] for y in [vi.split('-') for vi in years if len(vi.split('-')) > 0] if y[0].isdigit()], key=lambda x: int(x)))
                    end_years = '#'.join(sorted([y[1].strip()[:4] for y in [vi.split('-') for vi in years if len(vi.split('-')) > 1] if y[1].strip()[:4].isdigit() and y[1].strip()[:4] != '9999'], key=lambda x: int(x)))

                if start_years is not None and len(start_years) >= 0:
                    normalized_values.append(start_years)
                else:
                    normalized_values.append('')

                if end_years is not None and len(end_years) >= 0:
                    normalized_values.append(end_years)
                else:
                    normalized_values.append('')

                cleaned_values = '\t'.join([v.replace('\t', ' ').strip() for v in normalized_values])

                tmp_cleaned_data.append(cleaned_values)
                len_included_lines += 1
            else:
                len_ignored_lines += 1
            line = file_tmp_original_data.readline()
        file_tmp_original_data.close()
    except FileNotFoundError:
        print('Please, provide a valid CSV file')
    print('%s lines were discarded. %s lines were considered' % (len_ignored_lines, len_included_lines))
    return tmp_cleaned_data


if __name__ == '__main__':

    if len(sys.argv) == 2:
        path_csv = sys.argv[1]
    else:
        print('Please, provide a CSV file')
        sys.exit(1)

    print('Filtering data')
    cleaned_data = clean_csv(path_csv)

    print('Saving cleaned data on disk')
    file_csv_cleaned = open(path_csv.split('.csv')[0] + '_cleaned_v0.4.csv', 'w')
    file_csv_cleaned.write('\t'.join(NORMALIZED_KEYS) + '\n')

    for c in cleaned_data:
        file_csv_cleaned.write(c + '\n')
    file_csv_cleaned.close()
