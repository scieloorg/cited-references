import sys

from pymongo import MongoClient
from util.string_processor import StringProcessor
from xylose.scielodocument import Citation


if __name__ == '__main__':
    db_name = sys.argv[1]
    indexes_base = sys.argv[2]

    refdb = MongoClient()[db_name]

    findex_base = [i.strip().split('\t') for i in open(indexes_base)]
    title2issnl = {}
    for r in findex_base:
        title = r[0]
        issnls = r[1]
        title2issnl[title] = issnls

    results = open('matches.tsv', 'w')
    results_titles = open('all_titles.tsv', 'w')
    results_issns_matched = open('issns_matched.tsv', 'w')
    results_titles_not_matched = open('titles_not_matched.tsv', 'w')

    titles = {}
    titles_matched = {}
    titles_not_matched = {}
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
                    else:
                        if cit_title_preprocessed not in titles_not_matched:
                            titles_not_matched[cit_title_preprocessed] = 1
                        else:
                            titles_not_matched[cit_title_preprocessed] += 1

                num_all += 1
        print('\n%d / %d (%.2f)' % (num_articles, num_all, float(num_articles)/float(num_all)))

    results.close()

    for t in sorted(titles, key=lambda x: titles[x], reverse=True):
        results_titles.write('\t'.join([t, str(titles[t])]) + '\n')
    results_titles.close()

    for t in titles_matched:
        for k in sorted(titles_matched[t], key=lambda y: titles_matched[t][y], reverse=True):
            results_issns_matched.write('\t'.join([t, str(k), str(titles_matched[t][k])]) + '\n')
    results_issns_matched.close()

    for k in sorted(titles_not_matched, key=lambda g: titles_not_matched[g], reverse=True):
        results_titles_not_matched.write('\t'.join([k, str(titles_not_matched[k])]) + '\n')
