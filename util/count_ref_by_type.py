from pymongo import MongoClient


def check_ref_type(json_data):
    if 'v30' in json_data:
        return u'article'
    elif 'v53' in json_data:
        return u'conference'
    elif 'v18' in json_data:
        if 'v51' in json_data:
            return u'thesis'
        else:
            return u'book'
    elif 'v150' in json_data:
        return u'patent'
    elif 'v37' in json_data:
        return u'link'
    else:
        return u'undefined'
        

if __name__ == "__main__":
    ref_scielo = MongoClient()['ref_scielo']

    collections = ref_scielo.list_database_names()

    for cname in ref_scielo.collection_names():
        collections[cname] = {'article':0,'conference':0,'thesis':0,'patent':0,'link':0,'undefined':0, 'book':0}
        for ref in ref_scielo[cname].find({}):    
            collections[cname][check_ref_type(p)] += 1

    for col in sorted(collections):
        for ref_type in sorted(collections.get(col)):
            print(col, ref_type, collections.get(col).get(ref_type))
            