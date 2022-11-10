import argparse
import os

from articlemeta.client import RestfulClient


COLLECTIONS = ['arg', 'bio', 'bol', 'books', 'cci', 'chl ', 'cic', 'col ', 'cri', 'cub', 'ecu', 'edc', 'esp', 'inv', 'mex', 'pef', 'per', 'ppg', 'pro', 'prt', 'pry', 'psi', 'rve', 'rvo', 'rvt', 'scl', 'ses', 'spa', 'sss', 'sza', 'ury', 'ven', 'wid']
DEFAULT_DIR_CSV = 'data/scielo/'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['journal', 'issue'], required=True)

    args = parser.parse_args()

    if not os.path.exists(DEFAULT_DIR_CSV):
        os.makedirs(DEFAULT_DIR_CSV)

    if args.mode == 'journal':
        with open(os.path.join(DEFAULT_DIR_CSV, 'scielo.csv'), 'w') as fout:
            fout.write('|'.join([
                'col',
                'scielo_issn',
                'print_issn',
                'electronic_issn',
                'title',
                'abbreviated_iso_title',
                'abbreviated_title',
                'acronym',
                'title_nlm',
                'subtitle',
                'previous_title',
                'next_title',
                'subject_areas',
                'subject_descriptors',
                'wos_subject_areas',
                'other_titles']) + '\n')

            cl = RestfulClient()

            for col in COLLECTIONS:
                for journal in cl.journals(collection=col):
                    print('|'.join([col, journal.scielo_issn, journal.title]))

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

                    fout.write(col + '|' + '|'.join(journal_info) + '\n')
                    fout.flush()

    if args.mode == 'issue':
        with open(os.path.join(DEFAULT_DIR_CSV, 'scielo-issues.csv'), 'w') as fout:
            fout.write('|'.join([
                'col',
                'scielo_issn',
                'publication_date',
                'volume'
                ]) + '\n')

            cl = RestfulClient()

            issues=[]

            for col in COLLECTIONS:
                for issue in cl.issues(collection=col):
                    issn = issue.journal.scielo_issn or ''
                    pubdate = issue.publication_date or ''
                    vol = issue.volume or ''

                    if issn and pubdate and vol:
                        issue_str = '|'.join([col, issn, pubdate, vol])

                        print(issue_str)

                        if issue_str not in issues:
                            issues.append(issue_str)
                            fout.write(issue_str + '\n')
                            fout.flush()
