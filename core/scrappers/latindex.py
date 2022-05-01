import argparse
import asyncio
import os

from aiohttp import ClientSession
from aiohttp.client_exceptions import ContentTypeError, ServerDisconnectedError, ClientPayloadError
from asyncio.exceptions import TimeoutError
from multiprocessing.pool import Pool
from bs4 import BeautifulSoup


PROFILE_URL = 'https://www.latindex.org/latindex/ficha?folio={profile_id}'

DEFAULT_DIR_CSV = 'data/latindex/'
DEFAULT_DIR_HTML = 'data/latindex/html/'
DEFAULT_MAX_ATTEMPTS = 10
START_PROFILE_ID = 0
DEFAULT_MAX_PROFILE_ID = 35000
DEFAULT_NUM_THREADS = 3
DEFAULT_SEMAPHORE_LIMIT = 5


def html2dict(path_html_file: str):
    """
    Open, reads and converts a html file (the main parts) to a comma-separated string (commastr).
    :param path_html_file: path of the html file.
    :return: a dict where each key is the filename and the value is its comma-separated string representation.
    """
    profile_id = path_html_file.split('.')[0]
    html = open(DEFAULT_DIR_HTML + path_html_file).read()

    pairs = {}

    # there are, currently, more than one html starting tag in a same html file
    # here we are trying to get the main html part
    splitted_html = html.split('<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" '
                               '"http://www.w3.org/TR/html4/loose.dtd">')
    if len(splitted_html) > 1:
        main_html = splitted_html[1]
        soupped_html = BeautifulSoup(main_html, 'html.parser')
        journal_data = soupped_html.find(id='bloque1')
        if journal_data is not None:
            html_pairs = journal_data.find_all('tr')
            pairs[profile_id] = {}
            for i in html_pairs:
                # remove redundant new lines and split the string (there's one \n at the middle of the str
                splitted_i = i.text.strip().split('\n')

                if len(splitted_i) == 2:
                    key = splitted_i[0].replace(';', ',').replace(':', '-').strip()
                    value = splitted_i[1].replace(';', ',').replace(':', '-').strip()
                    pairs[profile_id][key] = value
    return pairs


def save_csv_file(id2data: list):
    """
    Save a dictionary into a csv file
    :param id2data: a list of dictionaries where each key is a profile_id and each value is the pairs of attribute's name and value
    """
    result_file = open(DEFAULT_DIR_CSV + 'latindex.csv', 'w')

    possible_attrs = set()
    for d in id2data:
        for v in d.values():
            for attr in v.keys():
                possible_attrs.add(attr)

    possible_attrs = sorted(possible_attrs)
    result_file.write(';'.join(['Profile Identifier'] + possible_attrs) + '\n')

    for d in id2data:
        keys = list(d.keys())
        if len(keys) > 0:
            profile_id = list(d.keys())[0]
            profile_csv_data = []
            for a in possible_attrs:
                profile_csv_data.append(d[profile_id].get(a, ''))
            result_file.write(';'.join([profile_id] + profile_csv_data) + '\n')

    result_file.close()


def save_html_file(path_html_file: str, response):
    """
    Saves a response into a html file.
    :param path_html_file: path of the html file to be created
    :param response: response in text format
    """
    html_file = open(path_html_file, 'w')
    html_file.writelines(response)
    html_file.close()
    print('arquivo %s coletado' % path_html_file)


async def fetch(url, profile_id, session):
    """
    Fetchs the url. Calls the method save_into_html_file with the response as a parameter (in text format).
    :param url: the url to be fetched
    :param profile_id: the id to be inserted into the url
    :param session: a ClientSession object
    """
    async with session.get(url.format_map({'profile_id': profile_id})) as response:
        try:
            for attempt in range(DEFAULT_MAX_ATTEMPTS):
                if response.status == 200:
                    response = await response.text()
                    save_html_file(DEFAULT_DIR_HTML + str(profile_id) + '.html', response)
                    break
                elif response.status == 500 and (attempt + 1) == DEFAULT_MAX_ATTEMPTS:
                    print('ResponseError', response.status, profile_id)
        except ServerDisconnectedError:
            print('ServerDisconnectedError', url)
        except TimeoutError:
            print('TimeoutError', url)
        except ContentTypeError:
            print('ContentTypeError', url)
        except ClientPayloadError:
            print('ClientPayloadError', url)


async def bound_fetch(sem, url, profile_id, session):
    """
    Limits the collecting task to a semaphore.
    :param sem: a controller of the maximum number of parallel connections
    :param url: the url to be fetched
    :param profile_id: the id to be inserted into the url
    :param session: a ClientSession object
    """
    async with sem:
        await fetch(url, profile_id, session)


async def run():
    """
    Creates tasks to get the csv file with respect to a list composed by profile ids (or a range).
    """
    url = PROFILE_URL
    sem = asyncio.Semaphore(DEFAULT_SEMAPHORE_LIMIT)
    tasks = []

    async with ClientSession() as session:
        for profile_id in range(START_PROFILE_ID, DEFAULT_MAX_PROFILE_ID):
            task = asyncio.ensure_future(bound_fetch(sem, url, profile_id, session))
            tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses


def _detect_last_id(directory_html):
    files = [f for f in os.listdir(directory_html) if f.endswith('.html')]
    return max([int(f.split('.')[0]) for f in files])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['collect', 'parse'], required=True)

    args = parser.parse_args()

    if args.mode == 'collect':
        if os.path.exists(DEFAULT_DIR_HTML):
            global START_PROFILE_ID
            START_PROFILE_ID = _detect_last_id(DEFAULT_DIR_HTML) + 1
            print('START_PROFILE_ID %d' % START_PROFILE_ID)
        else:
            os.makedirs(DEFAULT_DIR_CSV)
            os.makedirs(DEFAULT_DIR_HTML)

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run())
        loop.run_until_complete(future)

    if args.mode == 'parse':
        htmls = [f for f in os.listdir(DEFAULT_DIR_HTML) if f.endswith('.html')]
        with Pool(DEFAULT_NUM_THREADS) as p:
            id2data = p.map(html2dict, htmls)

        save_csv_file(id2data)
