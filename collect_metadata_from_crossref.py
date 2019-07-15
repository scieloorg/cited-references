#!/usr/bin/env python3
import asyncio
import json
import requests
import sys

from aiohttp import ClientSession
from asyncio import TimeoutError
from aiohttp.client_exceptions import ContentTypeError
from pymongo import MongoClient


def save_into_local_database(local_database, collection, response, ref_id):
    '''
    Receives a response (in json format) and its ref_id.
    Saves the document into the local database.
    Set status equal to 1 if the DOI exists in the Crossref.
    Set status equal to -1 if the DOI does not exist in the Crossref.
    '''
    query = { '_id': ref_id }
    new_data = { '$set': {
        'status': 1, 
        'crossref_metadata': response.get('message')
        }
    }
    local_database[collection].update_one(query, new_data)


async def fetch(local_database, collection, url, session, ref_id):
    '''
    Fetchs the url containing the doi code.
    Calls the method save_into_local_database with the response as a parameter (in json format).
    '''
    async with session.get(url) as response:
        try:
            response = await response.json()
            save_into_local_database(local_database, collection, response, ref_id)
        except TimeoutError:
            print('Error: timeout %s ' % (ref_id))
            query = { '_id': ref_id }
            new_data = { '$set': { 
                'status': -2
                }
            }
            local_database[collection].update_one(query, new_data)
        except ContentTypeError:
            print('Error: type %s ' % (ref_id))
            query = { '_id': ref_id }
            new_data = { '$set': { 
                'status': -1 
                }
            }
            local_database[collection].update_one(query, new_data)


async def bound_fetch(local_database, collection, sem, url, session, ref_id):
    '''
    Limits the collecting task to a semaphore.
    '''
    async with sem:
        await fetch(local_database, collection, url, session, ref_id)


async def run(local_database, collection, references_with_doi, email:str):
    '''
    Receives a cursor of references with doi and an e-mail for mounting the crossref request.
    Creates tasks to collect metadata and save them into the local database.
    '''
    url = 'https://api.crossref.org/works/{}'
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
    tasks = []

    async with ClientSession(headers={'mailto': email}) as session:
        for ref in references_with_doi:
            ref_doi = ref.get('v237')[0].get('_')
            ref_id = ref.get('_id')
            task = asyncio.ensure_future(bound_fetch(local_database, collection, sem, url.format(ref_doi), session, ref_id))
            tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses


def get_references_with_doi(ref_db_collection):
    '''
    Receives a references database's collection.
    Return a pymongo cursor to iterate over references with a doi code.
    '''
    return ref_db_collection.find({'v237': {'$exists': True}})


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('Error: please, enter the references database name')
        print('Error: please, enter the registered e-mail in the crossref service')
        sys.exit(1)

    REFERENCES_DATA_BASE_NAME = sys.argv[1]
    EMAIL = sys.argv[2]

    SEMAPHORE_LIMIT = 250

    local_mongo_client = MongoClient()
    local_database = local_mongo_client[REFERENCES_DATA_BASE_NAME]

    for collection in local_database.collection_names()[1:]:
        references_with_doi = get_references_with_doi(local_database[collection])
        print('there are %d references with doi in collection %s' % (references_with_doi.count(), collection))

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(local_database, collection, references_with_doi, EMAIL))
        loop.run_until_complete(future)
