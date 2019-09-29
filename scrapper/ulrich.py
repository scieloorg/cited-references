#!/usr/bin/env python3
import asyncio
from multiprocessing.pool import Pool

import bs4
import itertools
import logging
import sys
import os
import zipfile

from asyncio import TimeoutError
from aiohttp import ClientSession, ClientConnectionError
from aiohttp.client_exceptions import ContentTypeError, ServerDisconnectedError
from bs4 import BeautifulSoup

ROOT_URL = 'https://ulrichsweb.serialssolutions.com/titleDetails/{}'

DEFAULT_START_ID = 12515
DEFAULT_END_ID = 835018
DEFAULT_RANGE_1 = range(DEFAULT_START_ID, DEFAULT_END_ID)
DEFAULT_RANGE_2 = range(15793473, 15798807)
DEFAULT_RANGE_IDS = itertools.chain(DEFAULT_RANGE_1, DEFAULT_RANGE_2)

DEFAULT_DIR_HTML = 'data/ulrich/html/'

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_MODE = 'collect'
DEFAULT_NUM_THREADS = 4
DEFAULT_SEMAPHORE_LIMIT = 2

DEFAULT_ATTRS = {'bd_Title', 'bd_ISSN', 'bd_Format', 'bd_Frequency', 'bd_Country'}


def _find_all_tr_pairs(key: str, title_details, profile_id):
    try:
        return title_details.find('div', {'id': key}).find('table', {'class': 'resultsTable'}).find_all('tr')
    except AttributeError:
        logging.warning('ID %s (KEY) %s doest not have resultsTable' % (profile_id, key))


def _split_journal_attrs(attrs):
    if attrs:
        return [t.text.replace(':', '').strip().split('\n') for t in
                [k for k in attrs if isinstance(k, bs4.element.Tag)]]
    return []


def _get_title_history(history_attrs):
    all_td = []
    if history_attrs:
        for h in history_attrs:
            all_td.extend(h.find_all('td'))
    if len(all_td) > 0:
        return '#'.join([''.join([a.strip() for a in k.text.split('\n')]) for k in all_td if isinstance(k, bs4.element.Tag)])
    return ''


def _get_pair_key_values(splitted_attrs, prefix: str):
    tmp_dict = {}
    for j in splitted_attrs:
        tmp_dict[prefix + j[0].replace('\t', ' ')] = '#'.join(
            [k.strip().replace('\t', ' ').replace('#', ' ') for k in j[1:] if k.strip() != ''])
    return tmp_dict


def html2dict(path_zip_file: str):
    """
    Open, reads and converts a zipped html into a dict.
    :param path_zip_file: path of the zip file
    :return: a dict where each key is the profile id and the value is its key-value pairs (attrs)
    """
    profile_id = path_zip_file.split('/')[-1].split('.')[0]
    inner_html_path = 'data/ulrich/html/' + profile_id + '.html'
    html_content = zipfile.ZipFile(path_zip_file).open(inner_html_path).read()

    parsed_data = [profile_id]

    soupped_html = BeautifulSoup(html_content, 'html.parser')

    title_details = soupped_html.find('div', {'id': 'resultPane'})
    basic_description_attrs = _find_all_tr_pairs('basicDescriptionContainer', title_details, profile_id)
    title_history_attrs = _find_all_tr_pairs('titleHistoryContainer', title_details, profile_id)
    bd_splitted = _split_journal_attrs(basic_description_attrs)
    dict_bd = _get_pair_key_values(bd_splitted, 'bd_')
    title_history = _get_title_history(title_history_attrs)

    for k in sorted(DEFAULT_ATTRS):
        parsed_data.append(dict_bd.get(k, ''))

    parsed_data.append(title_history)

    return parsed_data


def save_tsv_file(parsed_data):
    """
    Save a parsed journal to a tsv file
    :param parsed_data: a list of dictionaries where the only main key is a profile_id and its value is the pairs of journal's attributes
    """
    result_file.write('\t'.join(parsed_data) + '\n')


def save_into_html_file(path_html_file: str, response):
    """
    Receives a response (in text format).
    Saves the document into a html file.
    """
    html_file = open(path_html_file, 'w')
    html_file.writelines(response)
    html_file.close()

    with zipfile.ZipFile(path_html_file.replace('.html', '.zip'), 'w') as zf:
        zf.write(path_html_file, compress_type=zipfile.ZIP_DEFLATED)
        zf.close()
    os.remove(path_html_file)


async def fetch(url, session):
    """
    Fetches the url.
    Calls the method save_into_html_file with the response as a parameter (in text format).
    """
    try:
        async with session.get(url) as response:
            profile_id = url.split('/')[-1]
            print('COLLECTING %s' % profile_id)
            for attempt in range(DEFAULT_MAX_ATTEMPTS):
                try:
                    if response.status == 200:
                        response = await response.text(errors='ignore')
                        save_into_html_file(DEFAULT_DIR_HTML + profile_id + '.html', response)
                        logging.info('COLLECTED: %s' % profile_id)
                        break
                    elif response.status == 500 and attempt == DEFAULT_MAX_ATTEMPTS:
                        logging.info('RESPONSE_ERROR_500: %s' % profile_id)
                    elif response.status == 404:
                        logging.info('RESPONSE_ERROR_404: %s' % profile_id)
                except ServerDisconnectedError:
                    logging.info('SERVER_DISCONNECTED_ERROR: %s' % profile_id)
                except TimeoutError:
                    logging.info('TIMEOUT_ERROR: %s' % profile_id)
                except ContentTypeError:
                    logging.info('CONTENT_TYPE_ERROR: %s' % profile_id)
    except TimeoutError:
        logging.info('GENERALIZED_TIMEOUT_ERROR')
    except ClientConnectionError:
        logging.info('GENERALIZED_CLIENT_CONNECTION_ERROR')
    except ServerDisconnectedError:
        logging.info('GENERALIZED_SERVER_DISCONNECTED_ERROR')
    except ContentTypeError:
        logging.info('GENERALIZED_CONTENT_TYPE_ERROR')


async def bound_fetch(sem, url, session):
    """
    Limits the collecting task to a semaphore.
    """
    async with sem:
        await fetch(url, session)


async def run():
    """
    Creates tasks to get the html file with respect to a list composed by htmls.
    """
    sem = asyncio.Semaphore(DEFAULT_SEMAPHORE_LIMIT)
    tasks = []

    async with ClientSession() as session:
        for u in [ROOT_URL.format(jid) for jid in DEFAULT_RANGE_IDS]:
            task = asyncio.ensure_future(bound_fetch(sem, u, session))
            tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses


if __name__ == "__main__":
    logging.basicConfig(filename='ulrich.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    MODE = sys.argv[1]
    DIR_HTML = sys.argv[2]

    if MODE == 'collect':
        DEFAULT_DIR_HTML = DIR_HTML
        os.makedirs(DEFAULT_DIR_HTML, exist_ok=True)

        if len(sys.argv) == 4:
            start_id = int(sys.argv[3])
            DEFAULT_RANGE_IDS = itertools.chain(range(start_id, DEFAULT_END_ID), DEFAULT_RANGE_2)

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run())
        loop.run_until_complete(future)
    elif MODE == 'parse':
        DEFAULT_DIR_HTML = DIR_HTML

        START = int(sys.argv[3])
        END = int(sys.argv[4])

        if END > len(os.listdir(DEFAULT_DIR_HTML)):
            END = len(os.listdir(DEFAULT_DIR_HTML))

        htmls = sorted([DEFAULT_DIR_HTML + h for h in os.listdir(DIR_HTML)])[START:END]

        result_file = open(DEFAULT_DIR_HTML + '../' + str(START) + '.tsv', 'w')
        result_file.write('\t'.join(['Profile Identifier'] + sorted(DEFAULT_ATTRS) + ['title_history']) + '\n')

        for i, h in enumerate(sorted(htmls)):
            print('\r%d / %d' % (i + 1 + START, START + len(htmls)), end='')
            parsed = html2dict(h)
            save_tsv_file(parsed)
        result_file.close()
