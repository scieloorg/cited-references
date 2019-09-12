#!/usr/bin/env python3
import asyncio
import bs4
import itertools
import logging
import sys
import os
import time
import zipfile

from asyncio import TimeoutError
from aiohttp import ClientSession
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
DEFAULT_SEMAPHORE_LIMIT = 2


# TODO: change the following method to attend ulrich web pages
def parse_html(path_html_file: str):
    """
    Open, reads and converts a html file (the main parts) to a tab-separated string.
    :param path_html_file: path of the html file.
    :return: a list of tab-separated string representations of the journals described in the html file.
    """
    page_id = path_html_file.split('.')[0]
    html = open(DEFAULT_DIR_HTML + path_html_file).read()
    soupped_html = BeautifulSoup(html, 'html.parser')

    # page's name
    page_name = soupped_html.find('p').text.strip().split('\n')[0]

    # journal's name
    journals_names = [dt.text.strip().split('.')[-1].strip().replace('\t', '') for dt in soupped_html.find_all('dt')]

    # periodicity, issn and e-issn
    journals_time_and_issns = []
    for dt in soupped_html.find_all('dt'):
        new_jii = ''
        for jii in dt.next_siblings:
            if isinstance(jii, bs4.element.Tag):
                if jii.name == 'dd':
                    break
            else:
                new_jii = new_jii + '\t' + jii.strip()
        journals_time_and_issns.append(new_jii)

    # other journal's attributes
    journals_other_attrs = [dd.text.strip().replace('\n', '\t') for dd in soupped_html.find_all('dd')]

    tsv_list = []
    for i, j in enumerate(journals_names):
        tsv_j = '\t'.join([page_id, page_name, j.replace('\t', ''), journals_time_and_issns[i], journals_other_attrs[i], '\n'])
        tsv_list.append(tsv_j)
    return tsv_list


def save_tsv_file(journals_tsv: list):
    """
    Save a list of tsvs into a tsv file
    :param journals_tsv a list of tsvs where each tsv is a tab-separeted string representation of a journal
    """
    result_file = open('ulrich.tsv', 'w')
    result_file.writelines(journals_tsv)
    result_file.close()


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
    Fetchs the url.
    Calls the method save_into_html_file with the response as a parameter (in text format).
    """
    try:
        async with session.get(url) as response:
            profile_id = url.split('/')[-1]
            for attempt in range(DEFAULT_MAX_ATTEMPTS):
                try:
                    if response.status == 200:
                        response = await response.text(errors='ignore')
                        save_into_html_file(DEFAULT_DIR_HTML + profile_id + '.html', response)
                        logging.info('COLLECTED: %s' % profile_id)
                        break
                    elif response.status == 500 and attempt == DEFAULT_MAX_ATTEMPTS:
                        print('RESPONSE_ERROR_500: %s [waiting 10 seconds to try again...]' % url)
                        logging.error('RESPONSE_ERROR: %s' % profile_id)
                        time.sleep(10)
                except ServerDisconnectedError:
                    print('SERVER_DISCONNECT_ERROR %s' % profile_id)
                    logging.error('SERVER_DISCONNECTED_ERROR: %s' % profile_id)
                except TimeoutError:
                    print('TIMEOUT_ERROR: %s' % profile_id)
                    logging.error('TIMEOUT_ERROR: %s' % profile_id)
                except ContentTypeError:
                    print('CONTENT_TYPE_ERROR: %s' % profile_id)
                    logging.error('CONTENT_TYPE_ERROR: %s' % profile_id)
    except TimeoutError:
        logging.critical('GENERALIZED_TIMEOUT_ERROR')
    except ServerDisconnectedError:
        logging.critical('GENERALIZED_SERVER_DISCONNECTED_ERROR')
    except ContentTypeError:
        logging.critical('GENERALIZED_CONTENT_TYPE_ERROR')


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
        htmls = sorted([h for h in os.listdir(DIR_HTML)])

        journals_tsv = []
        for i, h in enumerate(htmls):
            print('parsing %d of %d' % (i + 1, len(htmls)))
            journals_tsv.extend(parse_html(h))

        print('saving file on disk')
        save_tsv_file(journals_tsv)
