#!/usr/bin/env python3
import asyncio
import bs4
import math
import sys
import os
import requests

from asyncio import TimeoutError
from aiohttp import ClientSession
from aiohttp.client_exceptions import ContentTypeError, ServerDisconnectedError
from bs4 import BeautifulSoup

ROOT_URL = 'http://mjl.clarivate.com'

DEFAULT_DIR_HTML = 'data/wos/html/'

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_MODE = 'collect'
DEFAULT_SEMAPHORE_LIMIT = 5


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
    result_file = open('wos.tsv', 'w')
    result_file.writelines(journals_tsv)
    result_file.close()


def get_url_indexes():
    response = requests.get(ROOT_URL)
    soup = BeautifulSoup(response.text, 'lxml')
    links = soup.find_all('a')
    return [ROOT_URL + l.get('href').replace('options', 'results') + '&mode=print' for l in links if '/cgi-bin/jrnlst/' in l.get('href')]


def save_into_html_file(path_html_file: str, response):
    """
    Receives a response (in text format).
    Saves the document into a html file.
    """
    html_file = open(path_html_file, 'w')
    html_file.writelines(response)
    html_file.close()


async def fetch(paged_url, session):
    """
    Fetchs the url.
    Calls the method save_into_html_file with the response as a parameter (in text format).
    """
    async with session.get(paged_url) as response:
        try:
            for attempt in range(DEFAULT_MAX_ATTEMPTS):
                if response.status == 200:
                    response = await response.text(errors='ignore')
                    path_html_file = paged_url.split('&')[0].split('?')[-1].lower().replace('=', '_') + '_' + paged_url.split('&')[-1].replace('=', '_').lower()
                    save_into_html_file(DEFAULT_DIR_HTML + path_html_file + '.html', response)
                    break
                elif response.status == 500 and attempt == DEFAULT_MAX_ATTEMPTS:
                    print('ResponseError', response.status, paged_url)
        except ServerDisconnectedError:
            print('ServerDisconnectedError', paged_url)
        except TimeoutError:
            print('TimeoutError', paged_url)
        except ContentTypeError:
            print('ContentTypeError', paged_url)


async def bound_fetch(sem, paged_url, session):
    """
    Limits the collecting task to a semaphore.
    """
    async with sem:
        await fetch(paged_url, session)


async def run():
    """
    Creates tasks to get the html file with respect to a list composed by htmls.
    """
    sem = asyncio.Semaphore(DEFAULT_SEMAPHORE_LIMIT)
    tasks = []

    async with ClientSession() as session:

        urls = get_url_indexes()

        for u in urls:
            tmp_index = requests.get(u)
            soup = BeautifulSoup(tmp_index.text, 'html.parser')
            allp = soup.find_all('p')[0].contents
            for a in allp:
                if 'Total journal' in a:
                    num_pages = math.ceil(int(a.strip().split(': ')[-1]) / 500)

            for page in range(1, num_pages + 1):
                paged_url = u + '&Page=' + str(page)
                task = asyncio.ensure_future(bound_fetch(sem, paged_url, session))
                tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses


if __name__ == "__main__":
    DEFAULT_MODE = sys.argv[1]
    PATH_TO_BE_PARSED = sys.argv[2]

    if len(sys.argv) != 3:
        print('Error: enter execution mode')
        print('Error: enter path to be parsed')
        sys.exit(1)

    if DEFAULT_MODE == 'collect':
        os.makedirs(DEFAULT_DIR_HTML)

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run())
        loop.run_until_complete(future)
    elif DEFAULT_MODE == 'parse':
        DEFAULT_DIR_HTML = PATH_TO_BE_PARSED
        htmls = sorted([h for h in os.listdir(PATH_TO_BE_PARSED)])

        journals_tsv = []
        for i, h in enumerate(htmls):
            print('parsing %d of %d' % (i + 1, len(htmls)))
            journals_tsv.extend(parse_html(h))

        print('saving file on disk')
        save_tsv_file(journals_tsv)
