import pickle


class FileManager(object):

    @staticmethod
    def get_col2pids_from_csv(path_pids_csv_file: str):
        """
        Receives the path of a collection,pid,doi file.
        Returns a dict where each key is the collection and each value is a list of pids.
        """
        file_pids = open(path_pids_csv_file)
        col2newpids = {}
        for line in file_pids:
            els = line.split(',')
            col = els[0]
            pid = els[1]
            if col not in col2newpids:
                col2newpids[col] = [pid]
            else:
                col2newpids[col].append(pid)
        return col2newpids

    @staticmethod
    def load_dict(path_file_dict: str):
        return pickle.load(open(path_file_dict, 'rb'))

    @staticmethod
    def save_dict(docs: dict, path_file_dict: str):
        """
        Receives a dict and the path of the file to be saved.
        Saves a dict into the disk.
        """
        file_dict = open(path_file_dict, 'wb')
        pickle.dump(docs, file_dict)

    @staticmethod
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

    @staticmethod
    def load_issnl2all_base(path_issnl2all_base: str):
        print('loading issnl2all base')
        base_issnl2all = [i.split('|') for i in open(path_issnl2all_base)]
        issn2issnl = {}
        issn2titles = {}
        for r in base_issnl2all[1:]:
            issns = r[1].split('#')
            issnl = r[0].strip()
            titles = r[2].split('#')
            for i in issns:
                if i not in issn2issnl:
                    issn2issnl[i] = issnl
                    issn2titles[i] = titles
                else:
                    if issn2issnl[i] != issnl:
                        print('ERROR: values (issnls) %s != %s for key (issn) %s' % (issnl, issn2issnl[i], i))
        del base_issnl2all
        return issn2issnl, issn2titles

    @staticmethod
    def load_year_volume_base(path_year_volume_base: str, issn2issnl: dict):
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

    @staticmethod
    def extract_slr_data(path_year_volume_base: str, i2l: dict):
        print('loading year volume base')
        year_volume_base = [i.split('|') for i in open(path_year_volume_base)]

        print('extracting year volume')
        issn2yv = {}
        title2yv = {}
        il2yv = {}
        for els in year_volume_base:
            issn = els[0].replace('-', '').strip()
            il = i2l.get(issn, issn)
            title = els[1].strip()
            year = els[2].strip()
            volume = els[3].strip()

            if year != '' and year.isdigit() and volume != '' and volume.isdigit():
                data = '|'.join([year, volume])

                if issn not in issn2yv:
                    issn2yv[issn] = [data]
                else:
                    issn2yv[issn].append(data)

                if title not in title2yv:
                    title2yv[title] = [data]
                else:
                    title2yv[title].append(data)

                if il not in il2yv:
                    il2yv[il] = [data]
                else:
                    il2yv[il].append(data)
        return issn2yv, title2yv, il2yv
