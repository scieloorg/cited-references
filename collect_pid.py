#!/usr/bin/env python3
import requests
import sys

from datetime import date


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

    COLLECTIONS = ['arg', 'bol ', 'chl ', 'col ', 'cri', 'cub', 'esp', 'mex', 'per', 'prt', 'rve', 'scl', 'psi', 'spa', 'sss', 'sza', 'ury', 'ven']
    ARTICLEMETA_URL = 'http://articlemeta.scielo.org'
    ARTICLE_ENDPOINT = '/api/v1/article'
    PATH_CSV = 'new-pids-from-' + from_date + '.csv'

    for c in COLLECTIONS:
        url = ARTICLEMETA_URL + ARTICLE_ENDPOINT + '/identifiers' + '/?collection=' + c
        if from_date:
            url += '&from=' + from_date

        results = requests.get(url).json()
        save_results_to_csv(results.get('objects'), PATH_CSV)

        total = int(results.get('meta').get('total'))
        
        if total > 1000:
            for o in range(1000, total, 1000):
                offset_url = url + '&offset=' + str(o)
                offset_results = requests.get(offset_url).json()
                save_results_to_csv(offset_results.get('objects'), PATH_CSV)
