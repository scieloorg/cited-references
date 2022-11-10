import json


CITATION_ROW_KEYS_SCIELO = [
    'citing_pid',
    'citing_issn',
    'citing_year',
    'cited_year',
    'cited_vol',
    'cited_journal',
    'cited_doi',
    'cited_doc_type',
    'cited_journal_std',
    'cited_num',
    'citing_ref_pid',
    'cited_source',
]

CITATION_ROW_KEYS_RM = [
    'citing_pid',
    'cited_year',
    'rm_solution',
    'volume',
]

CITATION_ROW_KEYS_ELSEVIER = [
    'line_id',
    'citing_srcid',
    'citing_issn_vars',
    'citationcount',
    'ref_pubyear',
    'ref_volume',
    'ref_issn_print',
    'ref_issn_electronic',
    'ref_sourcetitle_set',
    'ref_sourcetitle_abbrev_set',
    'cited_doiset',
]


class Citation:
    def __init__(self, data, format='json', keys=CITATION_ROW_KEYS_SCIELO):
        if format == 'json':
            self.load_from_json(data, keys)

        elif format == 'csv':
            self.load_from_csv(data)

    def load_from_csv(self, data):
        els = data.strip().split('|')
        setattr(self, 'citing_pid', els[0])
        setattr(self, 'cited_year', els[1][:5]) if len(els[1]) >= 4 else setattr(self, 'cited_year', els[1])
        setattr(self, 'cited_journal', els[2])
        setattr(self, 'cited_vol', els[4])

        if '^i' in els[3]:
            sb_cited_journal, sb_cited_issn = els[3].split('^i')
            setattr(self, 'sb_cited_journal', sb_cited_journal)
            setattr(self, 'sb_cited_issn', sb_cited_issn)
        elif '~~1' in els[3]:
            discard, sb_cited_journal = els[3].split('~~1_')
            setattr(self, 'sb_cited_journal', sb_cited_journal)

    def load_from_json(self, data, keys):
        try:
            json_data = json.loads(data)
        except json.JSONDecodeError:
            json_data = {'error': 'it was not possible to load the citation'}

        for k in json_data.keys():
            setattr(self, k, json_data[k])

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, ensure_ascii=False)

    def setattr(self, key, value):
        setattr(self, key, value)
