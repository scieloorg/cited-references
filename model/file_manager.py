import pickle


class FileManager(object):

    @staticmethod
    def get_col2pids_from_csv(path_pids_csv_file:str):
        '''
        Receives the path of a collection,pid,doi file.
        Returns a dict where each key is the collection and each value is a list of pids.
        '''
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
        '''
        Receives a dict and the path of the file to be saved.
        Saves a dict into the disk.
        '''
        file_dict = open(path_file_dict, 'wb')
        pickle.dump(docs, file_dict)
