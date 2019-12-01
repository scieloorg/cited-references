#!/usr/bin/env python3
from pymongo import MongoClient
from xylose.scielodocument import Article


def save_dict(name: str, pids: dict):
    results_file = open(name, 'w')
    for c in pids:
        for i in pids[c]:
            results_file.write(i + '\n')
    results_file.close()


if __name__ == '__main__':

    client = MongoClient()
    db = client['refSciELO_001']

    pids_with_doi = {}
    pids_without_doi = {}

    # add collection to the dictionaries
    for col in db.collection_names():
        pids_with_doi[col] = []
        pids_without_doi[col] = []

    # add pid to the respective dictionary (accordding to the existance or not of a DOI)
    for col in sorted(db.collection_names()):
        print('col %s' % col)
        for a in db[col].find():
            a1 = Article(a)
            col_pid = [col, a1.data['_id']]
            if a1.doi is not None:
                col_pid_doi = '|'.join(col_pid + [a1.doi])
                pids_with_doi[col].append(col_pid_doi)
            else:
                pids_without_doi[col].append('|'.join(col_pid))

    # save lists into the disk
    save_dict('pids_with_doi.csv', pids_with_doi)
    save_dict('pids_without_doi.csv', pids_without_doi)
