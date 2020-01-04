#!/usr/bin/env python3
import datetime
import multiprocessing as mp
import os
import re
import sys


def load_title2issnl_base(path_title2issnl_base: str):
    print('loading title2issnl base')
    base_title2issnl = [i.split('|') for i in open(path_title2issnl_base)]
    title2issnl = {}
    for r in base_title2issnl[1:]:
        title = r[0].strip()
        issnls = r[1].strip()
        title2issnl[title] = issnls
    del base_title2issnl
    return title2issnl


def fuzzy_match(title: str):
    words = title.split(' ')
    if len(title.replace('IMPRESSO', '').replace('ONLINE', '').replace('CDROM', '').replace('PRINT', '').replace('ELECTRONIC', '')) > 6 and len(words) >= 2:
        pattern = r'[\w|\s]*'.join([word for word in words]) + '[\w|\s]*'
        title_pattern = re.compile(pattern, re.UNICODE)
        fuzzy_matches = []
        for oficial_title in [ot for ot in title2issnl.keys() if ot.startswith(words[0])]:
            match = title_pattern.fullmatch(oficial_title)
            if match:
                fuzzy_matches.append(title2issnl[oficial_title])
        return set(fuzzy_matches)
    return set()


def worker(data):
    col = data[0]
    cit_id = data[1]
    cit_title = data[2]
    cit_year = data[3]
    cit_volume = data[4]
    fmatches = fuzzy_match(cit_title)
    if len(fmatches) > 0:
        res = '|'.join([col, cit_id, cit_title, cit_year, cit_volume, '#'.join(fmatches), str(len(fmatches))])
        results_fuzzy_matches.write(res + '\n')


if __name__ == '__main__':
    title2issnl = load_title2issnl_base(sys.argv[1])
    to_process = [l.strip().split('|') for l in open(sys.argv[2])]
    to_process_scielo = [t for t in to_process if t[0] == 'scl']

    matches_folder = '/'.join(['matches', str(round(datetime.datetime.utcnow().timestamp() * 1000))])
    os.makedirs(matches_folder)
    results_fuzzy_matches = open(matches_folder + '/fuzzy.tsv', 'w')

    with mp.Pool(mp.cpu_count()) as p:
        results = p.map(worker, to_process_scielo)
    results_fuzzy_matches.close()
