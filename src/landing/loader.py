from pymongo import MongoClient
import pandas as pd

def mongoimport(client_hdfs, hdfs_file_path, db_name = "Local_DB", coll_name = "BDM_project", db_url='localhost', db_port=27017):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documents in the new collection
    """
    mongo_client = MongoClient(db_url, db_port)
    db = mongo_client[db_name]
    coll = db[coll_name]

    with client_hdfs.read(hdfs_file_path) as reader:
        df = pd.read_csv(reader) if hdfs_file_path.endswith('.csv') else pd.read_json(reader)

    coll.delete_many({})
    coll.insert_many(df.to_dict('records'))

    return coll.count_documents({})

    mongo_client.close() #close connection
