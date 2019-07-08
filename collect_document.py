#!/usr/bin/env python3
import asyncio
import sys

from aiohttp import ClientSession
from pymongo import MongoClient


async def fetch(url, session, col, pid):
    '''
    Fetchs the url for the collection and pid passed as parameters.
    Calls the method save_into_local_database with the response as a parameter (in json format).
    '''
    async with session.get(url) as response:
        response = await response.json()
        save_into_local_database(response, col, pid)


async def bound_fetch(sem, url, session, col, pid):
    '''
    Limits the collecting task to a semaphore.
    '''
    async with sem:
        await fetch(url, session, col, pid)


async def run(col2pids={}):
    '''
    Receives a dictionary of collection (key): pid (values).
    Creates tasks to collect pids.
    '''
    url = 'http://articlemeta.scielo.org/api/v1/article/?collection={}&code={}&format=json'
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
    tasks = []

    async with ClientSession() as session:
        for col in col2pids:
            col_pids = col2pids.get(col)
            for p in col_pids:
                task = asyncio.ensure_future(bound_fetch(sem, url.format(col, p), session, col, p))
                tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses


def parse_new_pids_list(new_pids_list):
    '''
    Receives a list of new PIDs containing collection,pid,doi,processing_date.
    Returns a dictionary collection (key): list of pids (value).
    '''
    pids = [p.split(',') for p in open(new_pids_list)]
    col2pids = {}
    for p in pids:
        col = p[0]
        pid = p[1]
        if col not in col2pids:
            col2pids[col] = [pid]
        else:
            col2pids[col].append(pid)
    print('there are %s pids to be collected' % len(pids))
    return col2pids


def save_into_local_database(json_document, col, pid):
    '''
    Receives a document (in json format), its collection and pid.
    Saves the document into the local database.
    '''
    if json_document:
        json_document['_id'] = pid
        if not local_database[col].find_one({'_id': pid}):
            local_database[col].insert_one(json_document)
        else:
            print('%s,%s is already in the local database' % (col, pid))
    else:
        print('Could not collect col,pid %s,%s ' % (col, pid))
    

if __name__ == "__main__":
    if sys.argv.__len__() == 3:
        NEW_PIDS = sys.argv[1]
        LOCAL_DATABASE_NAME = sys.argv[2]
    else:
        print('Error: please, provide a list of new PIDs in CSV format, as follows: collection,code,doi,processind_date')
        print('Error: please, provide the name of the local database MongoDB (e.g., refSciELO_001')
        sys.exit(1)

    SEMAPHORE_LIMIT = 100

    col2pids = parse_new_pids_list(NEW_PIDS)

    local_mongo_client = MongoClient()
    local_database = local_mongo_client[LOCAL_DATABASE_NAME]

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(col2pids))
    loop.run_until_complete(future)
