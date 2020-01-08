#!/usr/bin/env python3
from time import sleep

import requests
import sys

from datetime import date
from json.decoder import JSONDecodeError


def save_results_to_csv(objects: {}, path_csv):
    """
    Receives a dict of PID objects and a path for a CSV file.
    Saves these objects in the CSV file.
    The CSV contains code, collection, doi and processing_date.
    If the path already contains a file, appends data to it.
    """
    file_results = open(path_csv, 'a')
    for obj in objects:
        code = obj.get('code')
        collection = obj.get('collection')
        doi = obj.get('doi')
        if not doi:
            doi = ''
        processing_date = obj.get('processing_date')
        file_results.write(','.join([collection, code, doi, processing_date]) + '\n')
    file_results.close()


if __name__ == "__main__":
    if sys.argv.__len__() == 2:
        from_date = sys.argv[1]
    else:
        from_date = date.today().strftime('%Y-%m-%d')

    COLLECTIONS = ['arg', 'bio', 'bol', 'cci', 'chl', 'cic', 'col', 'cri', 'cub', 'ecu', 'edc', 'esp', 'inv', 'mex', 'pef', 'per', 'ppg', 'pro', 'prt', 'pry', 'psi', 'rve', 'rvo', 'rvt', 'scl', 'ses', 'spa', 'sss', 'sza', 'ury', 'ven', 'wid']
    ARTICLEMETA_URL = 'http://articlemeta.scielo.org'
    ARTICLE_ENDPOINT = '/api/v1/article'
    PATH_CSV = 'new-pids-from-' + from_date + '.csv'

    for c in COLLECTIONS:
        print('COLLECTION %s' % c)
        url = ARTICLEMETA_URL + ARTICLE_ENDPOINT + '/identifiers' + '/?collection=' + c
        if from_date:
            url = url + '&from=' + from_date

        print(url)
        main_response = requests.get(url)
        results = main_response.json()
        save_results_to_csv(results.get('objects'), PATH_CSV)

        total = int(results.get('meta').get('total'))
        
        if total > 1000:
            for o in range(1000, total, 1000):
                tries = 1
                offset_url = url + '&offset=' + str(o)
                try:
                    response = requests.get(offset_url)
                    while response.status_code != 200 and tries <= 5:
                        response = requests.get(offset_url)
                        print('TRY %d' % tries)
                        tries += 1
                        sleep(5)

                    if response.status_code == 200:
                        offset_results = response.json()
                        save_results_to_csv(offset_results.get('objects'), PATH_CSV)
                    else:
                        print('URL NOT FOUND %s ' % offset_url)
                except JSONDecodeError as jde:
                    print(jde)
