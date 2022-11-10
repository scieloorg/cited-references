#!/usr/bin/env python3


def parse_nlm_journals(path_csv_nlm):
    raw_nml_journals = [l for l in open(path_csv_nlm)]

    jid2metada = {}

    # each resgister is composed of 8 lines
    for i in range(0, len(raw_nml_journals), 8):
        # the first element is the attrib's name and the second is its value
        jr_id = raw_nml_journals[i + 1].strip().split('JrId:')[1].replace('\t', ' ').strip()
        jr_title = raw_nml_journals[i + 2].strip().split('JournalTitle:')[1].replace('\t', ' ').strip()
        jr_med_abbr = raw_nml_journals[i + 3].strip().split('MedAbbr:')[1].replace('\t', ' ').strip()
        jr_issn_print = raw_nml_journals[i + 4].strip().split('ISSN (Print):')[1].replace('\t', ' ').strip()
        jr_issn_online = raw_nml_journals[i + 5].strip().split('ISSN (Online):')[1].replace('\t', ' ').strip()
        jr_iso_abbr = raw_nml_journals[i + 6].strip().split('IsoAbbr:')[1].replace('\t', ' ').strip()
        jr_nlm_id = raw_nml_journals[i + 7].strip().split('NlmId:')[1].replace('\t', ' ').strip()

        if jr_id not in jid2metada:
            jid2metada[jr_id] = [jr_title, jr_med_abbr, jr_issn_print, jr_issn_online, jr_iso_abbr, jr_nlm_id]
        else:
            if jr_title != jid2metada[jr_id][0]:
                print(jr_id, jr_title, jid2metada[jr_id])

    return jid2metada


if __name__ == '__main__':
    file_entrez = '/home/rafael/Temp/scielo/bases/originais/nlm/J_Entrez.txt'
    jid2metadata = parse_nlm_journals(file_entrez)

    file_entrez_cleaned = open('/home/rafael/Temp/scielo/bases/limpas/v0.4/nlm.csv', 'w')
    file_entrez_cleaned.write('\t'.join(['jr_id', 'jr_title', 'jr_med_abbr', 'jr_issn_print', 'jr_issn_online', 'jr_iso_abbr', 'jr_nlm_id']) + '\n')

    for e in jid2metadata:
        file_entrez_cleaned.write('\t'.join([e] + jid2metadata[e]) + '\n')

    file_entrez_cleaned.close()
