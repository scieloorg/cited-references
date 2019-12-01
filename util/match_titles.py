#!/usr/bin/env python3
import datetime
import os
import sys
sys.path.append(os.getcwd())

from pymongo import MongoClient
from util.string_processor import StringProcessor
from xylose.scielodocument import Citation


def exact_match():
    pass


def fuzzy_match():
    pass


def load_title2issnl_base(path_title2issnl_base: str):
    print('loading title2issnl base')
    base_title2issnl = [i.split('|') for i in open(path_title2issnl_base)]
    title2issnl = {}
    for r in base_title2issnl[1:]:
        title = r[0].strip()
        issnls = r[1].strip()
        title2issnl[title] = issnls
    del base_title2issnl
    return title2issnl


def load_issnl2all_base(path_issnl2all_base: str):
    print('loading issnl2all base')
    base_issnl2all = [i.split('|') for i in open(path_issnl2all_base)]
    issn2issnl = {}
    for r in base_issnl2all[1:]:
        issns = r[1].split('#')
        issnl = r[0]
        for i in issns:
            if i not in issn2issnl:
                issn2issnl[i] = issnl
            else:
                if issn2issnl[i] != issnl:
                    print('ERROR: values (issnls) %s != %s for key (issn) %s' % (issnl, issn2issnl[i], i))
    del base_issnl2all
    return issn2issnl


def load_year_volume_base(path_year_volume_base: str):
    print('loading year_volume_base')
    year_volume_base = [i.split('|') for i in open(path_year_volume_base)]
    title_year_volume2issn = {}
    for r in year_volume_base:
        title = r[1].strip()
        issn = r[0].strip().replace('-', '')
        normalized_issn = issn2issnl.get(issn, '')
        if normalized_issn == '':
            normalized_issn = issn
        year = r[2].strip()
        volume = r[3].strip()
        mkey = '-'.join([title, year, volume])
        if mkey not in title_year_volume2issn:
            title_year_volume2issn[mkey] = {normalized_issn}
        else:
            title_year_volume2issn[mkey].add(normalized_issn)
    del year_volume_base
    return title_year_volume2issn


if __name__ == '__main__':

    # local database name
    db_name = sys.argv[1]

    # paths of the correctional base files
    path_title2issnl_base = sys.argv[2]
    path_issnl2all_base = sys.argv[3]
    path_year_volume_base = sys.argv[4]

    # load data files to dictionaries
    title2issnl = load_title2issnl_base(path_title2issnl_base)
    issn2issnl = load_issnl2all_base(path_issnl2all_base)
    title_year_volume2issn = load_year_volume_base(path_year_volume_base)

    # create folder where the results will be saved
    matches_folder = '/'.join(['matches', str(round(datetime.datetime.utcnow().timestamp()*1000))])
    os.makedirs(matches_folder)

    # create results files
    results = open(matches_folder + '/matches.tsv', 'w')
    results_titles = open(matches_folder + '/all_titles.tsv', 'w')
    results_issns_matched = open(matches_folder + '/issns_matched.tsv', 'w')
    results_titles_not_matched = open(matches_folder + '/titles_not_matched.tsv', 'w')
    results_b2sec_desambiguated = open(matches_folder + '/homonymous_disambiguated.tsv', 'w')

    # create dictionarires where the results will be added
    titles = {}
    titles_matched = {}
    titles_not_matched = {}

    # access local references' database
    refdb = MongoClient()[db_name]

    for col in refdb.list_collection_names():
        print('\nStart %s' % col)
        num_articles = 0
        num_all = 0
        for cjson in refdb[col].find({}):
            cit = Citation(cjson)
            if cit.source:
                if cit.publication_type == 'article':
                    print('\r%d' % num_articles, end='')
                    num_articles += 1
                    cit_title_preprocessed = StringProcessor.preprocess_journal_title(cit.source).upper()
                    cit_year = cit.publication_date
                    cit_volume = cit.volume

                    if cit_title_preprocessed not in titles:
                        titles[cit_title_preprocessed] = 1
                    else:
                        titles[cit_title_preprocessed] += 1

                    if cit_title_preprocessed in title2issnl:
                        res_issns = title2issnl.get(cit_title_preprocessed)
                        res_line = [col, cit.data.get('_id'), cit_title_preprocessed, res_issns, str(len(res_issns.split('#')))]
                        results.write('\t'.join(res_line) + '\n')

                        res_issns_els = res_issns.split('#')

                        if cit_title_preprocessed not in titles_matched:
                            titles_matched[cit_title_preprocessed] = {len(res_issns_els): 1}
                        else:
                            if len(res_issns_els) not in titles_matched[cit_title_preprocessed]:
                                titles_matched[cit_title_preprocessed][len(res_issns_els)] = 1
                            else:
                                titles_matched[cit_title_preprocessed][len(res_issns_els)] += 1

                        if len(res_issns_els) > 1:
                            if cit_year is not None and cit_volume is not None:
                                cit_mkey = '-'.join([cit_title_preprocessed, cit_year, cit_volume])
                                if cit_mkey in title_year_volume2issn:
                                    bsec_cit_issns = title_year_volume2issn[cit_mkey]
                                    if len(bsec_cit_issns) == 1:
                                        unique_issnl = list(bsec_cit_issns)[0]
                                        results_b2sec_desambiguated.write('\t'.join(res_line + [cit_mkey, unique_issnl, str(len(bsec_cit_issns))]) + '\n')
                                    else:
                                        multiple_issnl = list(bsec_cit_issns)
                                        results_b2sec_desambiguated.write('\t'.join(res_line + [cit_mkey] + ['#'.join(multiple_issnl), str(len(multiple_issnl))]) + '\n')
                    else:
                        if cit_title_preprocessed not in titles_not_matched:
                            titles_not_matched[cit_title_preprocessed] = 1
                        else:
                            titles_not_matched[cit_title_preprocessed] += 1

                num_all += 1
        print('\n%d / %d (%.2f)' % (num_articles, num_all, float(num_articles)/float(num_all)))

    results.close()
    results_b2sec_desambiguated.close()

    for t in sorted(titles, key=lambda x: titles[x], reverse=True):
        results_titles.write('\t'.join([t, str(titles[t])]) + '\n')
    results_titles.close()

    for t in titles_matched:
        for k in sorted(titles_matched[t], key=lambda y: titles_matched[t][y], reverse=True):
            results_issns_matched.write('\t'.join([t, str(k), str(titles_matched[t][k])]) + '\n')
    results_issns_matched.close()

    for k in sorted(titles_not_matched, key=lambda g: titles_not_matched[g], reverse=True):
        results_titles_not_matched.write('\t'.join([k, str(titles_not_matched[k])]) + '\n')
