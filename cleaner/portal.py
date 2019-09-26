#!/usr/bin/env python3
import sys

USEFUL_COLUMNS_INDEXES = [1, 4, 13, 22, 25, 35, 37, 39, 56, 57, 81, 83, 96, 102]
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
                sline = line.split(',')
                filtered_line = ','.join([sline[k] for k in USEFUL_COLUMNS_INDEXES])
                tmp_cleaned_data.append(filtered_line.strip())
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
    file_csv_cleaned = open(path_csv.split('.csv')[0] + '_cleaned.csv', 'w')

    for c in cleaned_data:
        file_csv_cleaned.write(c + '\n')
    file_csv_cleaned.close()
