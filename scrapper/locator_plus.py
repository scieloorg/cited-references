#!/usr/bin/env python3
import bs4
import os
import re
import requests
import sys
sys.path.append(os.getcwd())

from util.string_processor import StringProcessor


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


def _search_attribute(attribute: str, souped_html: bs4.BeautifulSoup):
    """
    Search for an attribute in the souped html.
    :param attribute: name of the attribute to be searched
    :param souped_html: variable of the souped html structure
    :return: a list containing the elements found
    """
    attrs = []
    for th in souped_html.find_all('th'):
        if th.get_text().lower() == attribute + ':':
            for i in th.next_elements:
                if isinstance(i, str):
                    if i.strip() != '':
                        attrs.append(i.strip().lower())
                elif isinstance(i, bs4.element.Tag):
                    if i.name == 'th':
                        break
    while attribute + ':' in attrs:
        attrs.remove(attribute + ':')
    return attrs


def _clean_availability(availability: list):
    """
    Clean list of availability data.
    :param availability: a list of availability data.
    :return: a cleaned version of the list of availability data (each row is a line which format follows 'year,volume,number')
    """
    pattern_volume_number_year = re.compile(r'(v\.)\s*(\d*)\s*(:)\s*(no.)\s*(\d*)\s*\(\s*(\d{4}\s*)\)', re.UNICODE)
    pattern_volume_year = re.compile(r'(v\.)\s*(\d*)\s*\(\s*(\d{4})\s*\)', re.UNICODE)
    pattern_volume_numbers_year = re.compile(r'(v\.)\s*(\d*)\s*(:)\s*(no.)\s*(\d*)([-|/])(\d*)\s*\(\s*(\d{4}\s*)\)', re.UNICODE)

    cleaned_availability = []
    for avs in availability:
        # capture volume, number and year
        for r in re.finditer(pattern_volume_number_year, avs):
            year = r.groups()[5]
            volume = r.groups()[1]
            number = r.groups()[4]
            yvn = '|'.join([year, volume, number])
            cleaned_availability.append(yvn)
        # capture volume and year
        for r in re.finditer(pattern_volume_year, avs):
            year = r.groups()[2]
            volume = r.groups()[1]
            yvn = '|'.join([year, volume, ''])
            cleaned_availability.append(yvn)
        # capture volume, a range of numbers and year
        for r in re.finditer(pattern_volume_numbers_year, avs):
            volume = r.groups()[1]
            number_start = r.groups()[4]
            separator = r.groups()[5]
            number_end = r.groups()[6]
            year = r.groups()[7]
            if number_start.isdigit():
                if number_end.isdigit():
                    if separator == '-':
                        for i in range(int(number_start), int(number_end) + 1):
                            yvni = '|'.join([year, volume, str(i)])
                            cleaned_availability.append(yvni)
                    elif separator == '/':
                        yvni_s = '|'.join([year, volume, str(number_start)])
                        yvni_e = '|'.join([year, volume, str(number_end)])
                        cleaned_availability.append(yvni_s)
                        cleaned_availability.append(yvni_e)
                else:
                    yvn = '|'.join([year, volume, str(number_start)])
                    cleaned_availability.append(yvn)
    return sorted(set(cleaned_availability))


def _clean_recent_issues(recent_issues: list):
    """
    Clean list of recent issues.
    :param recent_issues: a list containing recent issues of the journal
    :return: a cleaned version of the list of recent issues
    """
    pattern_volume_number_year = re.compile(r'(v\.)\s*(\d*)\s*(,)\s*(no.)\s*(\d*)\s*\([\w*|\.|\s|-]*\s*(\d{4})\s*\w*\)', re.UNICODE)
    pattern_volume_year = re.compile(r'(v\.)\s*(\d*)\s*[,|\s]\(\s*(\d{4})\s*\)', re.UNICODE)
    pattern_volume_numbers_year = re.compile(r'(v\.)\s*(\d*)\s*(,)\s*(no.)\s*(\d*)\s*-(\d*)\s*\([\w*|\.|\s|-]*\s*(\d{4})\s*\w*\)', re.UNICODE)
    cleaned_recent_issues = []
    for ri in recent_issues:
        # capture volume, number and year
        for r in re.finditer(pattern_volume_number_year, ri):
            year = r.groups()[5]
            volume = r.groups()[1]
            number = r.groups()[4]
            yvn = '|'.join([year, volume, number])
            cleaned_recent_issues.append(yvn)
        # capture volume and year
        for r in re.finditer(pattern_volume_year, ri):
            year = r.groups()[2]
            volume = r.groups()[1]
            yvn = '|'.join([year, volume, ''])
            cleaned_recent_issues.append(yvn)
        # capture volume, a range of numbers and year
        for r in re.finditer(pattern_volume_numbers_year, ri):
            volume = r.groups()[1]
            number_start = r.groups()[4]
            number_end = r.groups()[5]
            year = r.groups()[6]
            if number_start.isdigit():
                if number_end.isdigit():
                    for i in range(int(number_start), int(number_end) + 1):
                        yvni = '|'.join([year, volume, str(i)])
                        cleaned_recent_issues.append(yvni)
                else:
                    yvn = '|'.join([year, volume, str(number_start)])
                    cleaned_recent_issues.append(yvn)
    return sorted(set(cleaned_recent_issues))


def _normalize_issn(issn: str):
    for i in ISSN_NORMALIZING_LIST:
        issn = issn.replace(i, '')
    return issn.strip()


def parse_html(html: str):
    """
    Convert a content in html format to a comma-separated-value string.
    :param html: content in html format
    :return: a list of comma-separeted-value strs
    """

    # convert html to soup
    souped_html = bs4.BeautifulSoup(html, features='html.parser')

    # get title
    title = set([StringProcessor.preprocess_journal_title(t) for t in _search_attribute('title', souped_html)])

    # get title abbreviation
    title_abbrev = set([StringProcessor.preprocess_journal_title(t) for t in _search_attribute('title abbreviation', souped_html)])

    # get availability
    availability = _search_attribute('availability', souped_html)
    cleaned_availability = _clean_availability(availability)

    # get issn
    issns = [_normalize_issn(i.strip().upper()) for i in set(_search_attribute('issn', souped_html))]

    # get recent issues
    recent_issues = _search_attribute('recent issues', souped_html)

    # clean recent issues
    cleaned_recent_issues = _clean_recent_issues(recent_issues)

    # convert data to comma-string-value lines
    years_volumes_numbers = set(cleaned_recent_issues).union(set(cleaned_availability))
    titles = title.union(title_abbrev)

    csv_lines = []
    for t in titles:
        for yvn in years_volumes_numbers:
            for i in issns:
                if i != '':
                    csv_lines.append(i + '|' + t.upper() + '|' + yvn.upper())
    return sorted(set(csv_lines))


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
    Read file J_Entrez.txt dowloaded from NLM Catalog.
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
    # a list to be used to normalizing a issn code
    ISSN_NORMALIZING_LIST = ['-', '(PRINT)', '( PRINT)', '(ELECTRONIC)', '(CDROM)', '(ONLINE)', '(OTHER)']

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
            print('parsing %s' % f)
            f_html = open(DIR_IN_HTMLS + '/' + f).read()
            parsed_htmls.append(parse_html(f_html))

        # file where each html file parsed will be saved
        file_csv = open('lp_parsed.csv', 'w')
        for p in parsed_htmls:
            for l in p:
                file_csv.write(l + '\n')
        file_csv.close()
