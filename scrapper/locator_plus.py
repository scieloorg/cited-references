#!/usr/bin/env python3
import bs4
import os
import requests
import sys


def get_html(title: str):
    """
    Search for the title in the locator plus service.
    :param title: the title to be searched
    :return: the content in html-text format
    """
    print('%s' % title)
    searched_url = LOCALTOR_PLUS_ENDPOINT.format(title, 1)
    res = requests.get(searched_url)
    if res.status_code == 200:
        if 'There is no bib data attached to this record.' not in res.text and 'Your search resulted in no hits!' not in res.text:
            return res.text
        else:
            print('\tWARNING: there is no data')
    else:
        print('\tWARNING: status code %s' % res.status_code)


def parse_html(html: str):
    """
    Convert a content in html format to a comma-separated-value string
    :param html: content in html format
    :return: a comma-separeted-value string
    """
    css = []
    souped_html = bs4.BeautifulSoup(html)

    return ','.join(css)


def save_html(title: str, html: str):
    """
    Save html content into a file named title.
    :param title: title of the filename to be saved
    :param html: content to be saved into the html file
    """
    normalized_title = title.replace('/', ' ').replace('\\', ' ')
    s = open('locator_plus_data/' + normalized_title + '-' + '1' + '.html', 'w')
    s.writelines(html)
    s.close()


def read_nlm_titles(file_nlm_titles: str):
    """
    Read file J_Entrez.txt dowloaded from NLM Catalog
    :param file_nlm_titles: a file containing MedAbbr: title of NLM Journals
    :return:
    """
    f = open(file_nlm_titles)
    line = f.readline()
    titles = []
    while line != '':
        if 'MedAbbr' in line:
            t = line[8:].strip().lower()
            if t != '':
                titles.append(t)
        line = f.readline()
    return titles


if __name__ == '__main__':
    # mode of execution (collect or parse)
    MODE = sys.argv[1]
    if MODE == 'collect':
        # file containing NLM titles
        FILE_IN_NLM_TITLES = sys.argv[2]

        # localtor plus endpoint where the script access to download each searched title
        LOCALTOR_PLUS_ENDPOINT = 'https://locatorplus.gov/cgi-bin/Pwebrecon.cgi?Search_Arg={}&SL=Submit%26LOCA%3DAll+NLM+Collections%7C11&Search_Code=JALL&CNT=100&HIST=1&v1={}'

        # dir where each html file downloaded will be saved
        os.makedirs('locator_plus_data/', exist_ok=True)

        titles = sorted(set(read_nlm_titles(FILE_IN_NLM_TITLES)))

        # start collecting from a specific title
        if len(sys.argv) == 4:
            START_TITLE = sys.argv[3].lower()
            START_TITLE_INDEX = titles.index(START_TITLE)
            if START_TITLE_INDEX == -1:
                START_TITLE_INDEX = 0
        else:
            START_TITLE_INDEX = 0

        for t in titles[START_TITLE_INDEX:]:
            t_content = get_html(t)
            if t_content is not None:
                save_html(t, t_content)

    elif MODE == 'parse':
        # directory containing html files downloaded
        DIR_IN_HTMLS = sys.argv[2]

        parsed_htmls = []
        for f in os.listdir(DIR_IN_HTMLS):
            parsed_htmls.append(parse_html(f))

        # file where each html file parsed will be saved
        file_csv = open('locator_plus_parsed.csv', 'w')
        for p in parsed_htmls:
            file_csv.write(p + '\n')
        file_csv.close()
