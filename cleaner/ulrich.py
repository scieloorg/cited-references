import csv
import os
import sys

def read_data(path_ulrich_data):
    print('reading %s' % path_ulrich_data)
    ulrich_data = set()
    with open(path_ulrich_data) as f:
        reader = csv.DictReader(f, delimiter='\t')
        for i in reader:
            title = i['bd_Title']
            issn = i['bd_ISSN']
            history_title = i['title_history']
            if issn != '' and title != '':
                ulrich_data.add('\t'.join([issn, title, history_title]))

    return ulrich_data

def save_data(data):
    print('saving %d lines' % len(data))
    with open('ulrich.csv', 'w') as f:
        for i in data:
            f.write(i + '\n')

if __name__ == '__main__':
    dir_ulrich_files = sys.argv[1]

    ulrich_data = set()

    for f in os.listdir(dir_ulrich_files):
        full_path = os.path.join(dir_ulrich_files, f)
        if full_path.endswith('.tsv'):
            ulrich_data = ulrich_data.union(read_data(full_path))

    save_data(ulrich_data)
