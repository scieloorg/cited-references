#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import sys

from asyncio import TimeoutError
from aiohttp import ClientSession, ClientConnectorError
from aiohttp.client_exceptions import ContentTypeError, ServerDisconnectedError
from pymongo import MongoClient


def save_into_local_database(local_database, collection, response, ref_id):
    """
    Receives a response (in json format) and its ref_id.
    Saves the document into the local database.
    Set status equal to 1 if the DOI exists in the Crossref.
    Set status equal to -1 if the DOI does not exist in the Crossref.
    """
    query = { '_id': ref_id }
    new_data = { '$set': {
        'status': 1, 
        'crossref_metadata': response.get('message')
        }
    }
    local_database[collection].update_one(query, new_data)


def save_into_disk(response, url):
    with open(PATH_RESULTS, 'a') as f:
        response['url_searched'] = url
        json.dump(response, f)
        f.write('\n')


async def fetch(local_database, collection, url, session, ref_id):
    """
    Fetchs the url containing the doi code.
    Calls the method save_into_local_database with the response as a parameter (in json format).
    """
    try:
        async with session.get(url) as response:
            try:
                response = await response.json()
                save_into_local_database(local_database, collection, response, ref_id)
            except (ServerDisconnectedError, TimeoutError):
                query = { '_id': ref_id }
                new_data = { '$set': {
                    'status': -2
                    }
                }
                local_database[collection].update_one(query, new_data)
            except ContentTypeError:
                if response.status == 404:
                    query = { '_id': ref_id }
                    new_data = { '$set': {
                        'status': -1
                        }
                    }
                    local_database[collection].update_one(query, new_data)
                else:
                    print('ref: %s status: %d' % (ref_id, response.status))
    except ServerDisconnectedError:
        print('server disconnected error %s' % ref_id)
    except TimeoutError:
        print('timeout error %s' % ref_id)
    except ContentTypeError:
        print('content type error %s' % ref_id)
    except ClientConnectorError:
        print('client connector error %s' % ref_id)


async def fetch_with_doi_list(url, session):
    """
    Fetchs the url containing the doi code.
    Calls the method save_into_local_database with the response as a parameter (in json format).
    """
    try:
        async with session.get(url) as response:
            try:
                response = await response.json()
                save_into_disk(response, url)
            except (ServerDisconnectedError, TimeoutError):
                logging.warning('ServerDisconnectedError %s' % url)
            except ContentTypeError:
                if response.status == 404:
                    logging.warning('DOINotFound %s' % url)
                elif response.status == 429:
                    logging.warning('TooManyRequests %s' % url)
                else:
                    logging.warning('ContentTypeError %s' % url)
    except ServerDisconnectedError:
        logging.warning('ServerDisconnectedError %s' % url)
    except TimeoutError:
        logging.warning('TimeoutError %s' % url)
    except ContentTypeError:
        logging.warning('ContentTypeError %s' % url)
    except ClientConnectorError:
        logging.warning('ClientConnectorError %s' % url)


async def bound_fetch(local_database, collection, sem, url, session, ref_id):
    """
    Limits the collecting task to a semaphore.
    """
    async with sem:
        await fetch(local_database, collection, url, session, ref_id)


async def bound_fetch_with_doi_list(sem, url, session):
    """
    Limits the collecting task to a semaphore.
    """
    async with sem:
        await fetch_with_doi_list(url, session)


async def run_with_doi_list(doi: list, email: str):
    url = 'https://api.crossref.org/works/{}'
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
    tasks = []

    async with ClientSession(headers={'mailto': email}) as session:
        for d in doi:
            task = asyncio.ensure_future(bound_fetch_with_doi_list(sem, url.format(d), session))
            tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses


async def run(local_database, collection, references_with_doi, email: str):
    """
    Receives a cursor of references with doi and an e-mail for mounting the crossref request.
    Creates tasks to collect metadata and save them into the local database.
    """
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
    """
    Returns the references that have doi code.
    Filter the results according to the STATUS_MODE.
    :param ref_db_collection: references database collection.
    :return: pymongo cursor to iterate over references with a doi code.
    """
    if STATUS_MODE == 'no-status':
        return ref_db_collection.find({'$and': [{'v237': {'$exists': True}}, {'status': {'$exists': False}}]})
    elif STATUS_MODE == '-1':
        return ref_db_collection.find({
            '$and': [
                {'v237': {'$exists': True}},
                {'$or': [
                    {'status': {'$exists': False}},
                    {'status': -1}]}]})
    elif STATUS_MODE == '-2':
        return ref_db_collection.find({
            '$and': [
                {'v237': {'$exists': True}},
                {'$or': [
                    {'status': {'$exists': False}},
                    {'status': -2}]}]})


if __name__ == "__main__":
    DEFAULT_MODE = sys.argv[1]

    if DEFAULT_MODE == '':
        REFERENCES_DATA_BASE_NAME = sys.argv[1]
        EMAIL = sys.argv[2]
        STATUS_MODE = sys.argv[3]

        SEMAPHORE_LIMIT = 10

        local_mongo_client = MongoClient()
        local_database = local_mongo_client[REFERENCES_DATA_BASE_NAME]

        for collection in local_database.collection_names():
            references_with_doi = get_references_with_doi(local_database[collection])
            print('there are %d references with doi in collection %s to be collected' % (references_with_doi.count(), collection))

            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(run(local_database, collection, references_with_doi, EMAIL))
            loop.run_until_complete(future)
    else:
        logging.basicConfig(filename='crossref.log', level=logging.WARNING, format='%(message)s')
        FILE_DOI_LIST = sys.argv[2]
        EMAIL = sys.argv[3]
        PATH_RESULTS = sys.argv[4]

        SEMAPHORE_LIMIT = 20

        ds = set([d.strip() for d in open(FILE_DOI_LIST)])
        print('there are %d DOIs' % len(ds))

        if os.path.exists(PATH_RESULTS):
            old_ds = set([json.loads(d.strip()).get('url_searched').replace('https://api.crossref.org/works/', '') for d in open(PATH_RESULTS)])
            ds = list(ds.difference(old_ds))

        print('there are %d DOIs to be collected' % len(ds))

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run_with_doi_list(ds, EMAIL))
        loop.run_until_complete(future)
