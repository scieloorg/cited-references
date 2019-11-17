#!/usr/bin/env python3
import logging
import sys
import os
sys.path.append(os.getcwd())

from util.string_processor import StringProcessor
from pymongo import MongoClient
from xylose.scielodocument import Article, UnavailableMetadataException


def save_metadata_scielo(data: set, path: str):
    fresults = open(path, 'w')
    for l in sorted(data):
        fresults.write(l + '\n')
    fresults.close()


if __name__ == '__main__':
    logging.basicConfig(filename='extract_year_volume.log', level=logging.INFO, format='%(message)s')

    client = MongoClient()
    doc_local_database = client['refSciELO_001']

    results = set()

    for col in sorted(doc_local_database.list_collection_names()):
        print('analysing collection %s' % col)
        db_doc_col = doc_local_database[col]
        for doc in db_doc_col.find({}):
            doc_results = []

            article = Article(doc)
            pid = article.data['_id']

            year = article.publication_date
            if year is None or year == '':
                break
            else:
                year = year.split('-')[0]

            try:
                volume = article.issue.volume
                if volume is None or volume == '':
                    break
            except UnavailableMetadataException as ume:
                logging.error('ERROR %s' % ume)

            try:
                numero = article.issue.number
                if numero is None:
                    numero = ''
            except UnavailableMetadataException as ume:
                logging.error('ERROR %s' % ume)

            issns = set()
            issns.add(article.journal.electronic_issn)
            issns.add(article.journal.print_issn)
            issns.add(article.journal.scielo_issn)
            issns = [i for i in issns if i is not None and i != '']

            titles = set()
            titles.add(StringProcessor.preprocess_journal_title(article.journal.abbreviated_iso_title))
            titles.add(StringProcessor.preprocess_journal_title(article.journal.abbreviated_title))
            titles.add(StringProcessor.preprocess_journal_title(article.journal.title))
            titles = [t for t in titles if t is not None and t != '']

            for t in sorted(titles):
                for i in issns:
                    row = '|'.join([i, t.upper(), year, volume, numero])
                    results.add(row)
                    logging.info(row)

    print('saving data')
    save_metadata_scielo(results, 'scielo_year_volume.tsv')
