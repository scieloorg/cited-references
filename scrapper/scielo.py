#!/usr/bin/env python3
import datetime

from articlemeta.client import RestfulClient


if __name__ == "__main__":

    today = datetime.datetime.today()
    file_results = open('-'.join(['journals_scielo', str(today.year), str(today.month), str(today.day)]) + '.csv', 'w')
    file_results.write('\t'.join(['col', 'scielo_issn', 'print_issn', 'electronic_issn', 'title', 'abbreviated_iso_title', 'abbreviated_title', 'acronym', 'title_nlm', 'subtitle', 'previous_title', 'next_title', 'subject_areas', 'subject_descriptors', 'wos_subject_areas', 'other_titles']) + '\n')

    cl = RestfulClient()

    collections = ['arg', 'bio', 'bol', 'books', 'cci', 'chl ', 'cic', 'col ', 'cri', 'cub', 'ecu', 'edc', 'esp', 'inv', 'mex', 'pef', 'per', 'ppg', 'pro', 'prt', 'pry', 'psi', 'rve', 'rvo', 'rvt', 'scl', 'ses', 'spa', 'sss', 'sza', 'ury', 'ven', 'wid']

    for col in collections:
        for journal in cl.journals(collection=col):
            print(col, journal.scielo_issn, journal.title, sep='\t')

            # collect issn
            journal_issn = [journal.scielo_issn, journal.print_issn, journal.electronic_issn]
            journal_issn = ['' if j is None else j for j in journal_issn]

            # collect titles
            journal_titles = [journal.title, journal.abbreviated_iso_title, journal.abbreviated_title, journal.acronym, journal.title_nlm, journal.subtitle, journal.previous_title, journal.next_title]
            if journal.other_titles is not None and len(journal.other_titles) > 0:
                journal_titles.append('#'.join(journal.other_titles))
            else:
                journal_titles.append('')
            journal_titles = ['' if i is None else i for i in journal_titles]

            # collect subject areas
            journal_areas = []
            if journal.subject_areas is not None and len(journal.subject_areas) > 0:
                journal_areas.append('#'.join(journal.subject_areas))
            else:
                journal_areas.append('')

            # collect descriptors
            if journal.subject_descriptors is not None and len(journal.subject_descriptors) > 0:
                journal_areas.append('#'.join(journal.subject_descriptors))
            else:
                journal_areas.append('')

            # collect wos subject areas
            if journal.wos_subject_areas is not None and len(journal.wos_subject_areas) > 0:
                journal_areas.append('#'.join(journal.wos_subject_areas))
            else:
                journal_areas.append('')

            journal_info = journal_issn + journal_titles + journal_areas

            file_results.write(col + '\t' + '\t'.join(journal_info) + '\n')

    file_results.close()
