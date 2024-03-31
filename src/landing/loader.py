from pymongo import MongoClient
import pandas as pd
import json
import os

def read_data(file_path):
    root_ext_pair = os.path.splitext(file_path)
    if root_ext_pair[1] == '.csv':
        df = pd.read_csv(file_path)
    else:
        df = pd.read_json(file_path)
    return df

def mongoimport(file_path, db_name, coll_name, db_url='localhost', db_port=27017):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documents in the new collection
    """
    client = MongoClient(db_url, db_port)
    db = client[db_name]
    coll = db[coll_name]
    data = read_data(file_path)
    payload = json.loads(data.to_json(orient='records'))
    print(payload)
    coll.delete_many({})
    coll.insert_many(payload)
    return coll.count_documents({})
    client.close() #close connection

# function that given a file can deal with it (CSV, JSON)

mongoimport('../../data/lookup_tables/idealista_extended.csv', 'Local_DB', 'BDM_project')