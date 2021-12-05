import datetime
import os


def check_dir(path):
    if os.path.exists(path):
        raise Exception
    
    os.makedirs(path)


def generate_folder_name(path):
    return os.path.join(
        path,
        str(round(datetime.datetime.utcnow().timestamp()*1000))
    )


def open_files(path: str) -> dict:
    '''
    Returns
    -------
    dict
        {
            'matches': ...,
            'journal-titles': ...,
            'matched-issns': ...,
            'unmatched-titles': ...,
            'homonymous-disambiguated': ...,
            'fuzzy': ...,
            'fuzzy-todo': ...,
        }
    '''
    files_names = [
        'matches',
        'journal-titles',
        'matched-issns',
        'unmatched-titles',
        'homonymous-disambiguated',
        'fuzzy',
        'fuzzy-todo',
    ]

    dict_files = {}

    for fn in files_names:
        try:
            dict_files[fn] = open(os.path.join(path, fn + '.tsv'), 'w')
        except:
            ...

    return dict_files


def close_files(dict_files):
    for v in dict_files.values():
        try:
            v.close()
        except:
            ...


def load_title_to_issnl(path: str, sep='|'):
    with open(path) as fin:
        title_to_issnl = {}

        for line in fin:
            els = line.split(sep)

            title = els[0].strip()
            issnls = els[1].strip()

            title_to_issnl[title] = issnls
        
        return title_to_issnl


def load_issnl_to_all(path: str, sep1='|', sep2='#'):
    with open(path) as fin:
        issn_to_issnl = {}
        issn_to_titles = {}
    
        for line in fin:
            els = line.split(sep1) 
    
            issns = els[1].split(sep2)
            issnl = els[0]
            titles = els[2].split(sep2)
        
            for i in issns:
                if i not in issn_to_issnl:
                    issn_to_issnl[i] = issnl
                    issn_to_titles[i] = titles
                else:
                    if issn_to_issnl[i] != issnl:
                        print('ERROR: [ISSNL] %s != ISSN_TO_ISSNL[key] %s for key %s' % (issnl, issn_to_issnl[i], i))
         
    return issn_to_issnl, issn_to_titles


def load_year_volume(path: str, data: dict, sep='|'):
    with open(path) as fin:
        title_year_volume_to_issn = {}

        for line in fin:
            els = line.split(sep)

            title = els[1].strip()
            issn = els[0].strip().replace('-', '')
            normalized_issn = data.get(issn, '')

            if normalized_issn == '':
                normalized_issn = issn

            year = els[2].strip()
            volume = els[3].strip()
            
            mkey = '-'.join([
                title, 
                year, 
                volume
            ])

            if mkey not in title_year_volume_to_issn:
                title_year_volume_to_issn[mkey] = {normalized_issn}
            else:
                title_year_volume_to_issn[mkey].add(normalized_issn)

    return title_year_volume_to_issn
