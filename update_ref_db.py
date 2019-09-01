#!/usr/bin/env python3
import sys

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


def parse_pids_from_file(path_pids:str):
    """
    Receives the path of the csv file new-pids.
    The current format of each line is collection,pid,doi.
    Returns a dict where each key is a collection and each value is a list of pids.
    """
    file_pids = open(path_pids)
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


if __name__ == "__main__":
    if sys.argv.__len__() == 4:
        LOCAL_DOC_DATABASE_NAME = sys.argv[1]
        NEW_PIDS = sys.argv[2]
        LOCAL_REF_DATABASE_NAME = sys.argv[3]
    else:
        print('Error: please, provide the name of the local documents database MongoDB (e.g., refSciELO_001)')
        print('Error: please, provide the list of new pids (e.g., new-pids-from-2019-06-10)')
        print('Error: please, provide the name of the local references database MongoDB (e.g., ref_scielo)')
        sys.exit(1)
    
    mongo_client = MongoClient()
    doc_local_database = mongo_client[LOCAL_DOC_DATABASE_NAME]
    ref_local_database = mongo_client[LOCAL_REF_DATABASE_NAME]

    col2newpids = parse_pids_from_file(NEW_PIDS)
    print('there are %d new pids' % sum(
        [len(col2newpids.get(col)) for col in col2newpids.keys()]
        )
    )

    for col in col2newpids:
        db_doc_col = doc_local_database[col]
        print('updating collection into the references database for %s' % col)
        for doc_pid in col2newpids.get(col):
            doc = doc_local_database[col].find_one({'_id': doc_pid})
            citations = doc.get('citations')
            for ci in citations:
                ref_id = ci.get('v701')[0].get('_')
                ref_key = '_'.join([doc.get('code'), ref_id])
                ref_value = ci
                json_ref = ci
                json_ref['_id'] = ref_key
                try:
                    ref_local_database[col].insert_one(json_ref)
                except DuplicateKeyError:
                    print('%s is already in the database' % ref_key)
