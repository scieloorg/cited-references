#!/usr/bin/env python3
from pymongo import MongoClient

import json
import requests
import sys


def get_references_with_doi(ref_db):
    print('getting references with doi')
    references_with_doi = []
    for collection in ref_db.collection_names()[:1]:
        for cit in ref_db[collection].find():
            if cit.get('v237') is not None:
                if cit.get('v237')[0].get('_') is not None:
                    cit['collection'] = collection
                    references_with_doi.append(cit)
    print('there are %s references with doi' % len(references_with_doi))
    return references_with_doi


def get_metadata_from_crossref(ref_db, references: list, email: str):
    print('getting metadata from crossref and saving it to local references database')
    CROSS_REF_WORKS_ENDPOINT = 'https://api.crossref.org/works/'

    for index, ref in enumerate(references):
        print('\r%s: %s of %s' % (ref.get('collection'), index + 1, len(references)), end='')
        url = CROSS_REF_WORKS_ENDPOINT + ref.get('v237')[0].get('_')

        response_headers = requests.head(url, headers = {'mailto': email})

        if response_headers.status_code == 200:
            response = requests.get(url, headers = {'mailto': email})
            if response.status_code == 200:
                resp_json = json.loads(response.text)
                if resp_json.get('status') == 'ok':
                    save_metadata(ref_db, ref, resp_json.get('message'))
        else: 
            save_ref_without_metadata(ref_db, ref)
    return


def open_database(references_db_name: str):
    client = MongoClient()
    return client[references_db_name]


def save_ref_without_metadata(ref_db, reference):
    query = { '_id': reference.get('_id') }
    new_data = { '$set': {
        'status': -1
        } 
    }
    ref_db[reference.get('collection')].update_one(query, new_data)


def save_metadata(ref_db, reference, crossref_metadata):
    query = { '_id': reference.get('_id') }
    new_data = { '$set': {
         'status': 1, 
         'crossref_metadata': crossref_metadata
         }
    }
    ref_db[reference.get('collection')].update_one(query, new_data)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('Error: please, enter the references database name')
        print('Error: please, enter the registered e-mail in the crossref service')
        sys.exit(1)

    REFERENCES_DATA_BASE_NAME = sys.argv[1]
    EMAIL = sys.argv[2]

    # open database
    ref_db = open_database(REFERENCES_DATA_BASE_NAME)

    # get references with doi
    refs_with_doi = get_references_with_doi(ref_db)
    
    # get metadata and save it into references database
    get_metadata_from_crossref(ref_db, refs_with_doi, EMAIL)
    