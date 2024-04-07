from pathlib import Path
from typing import Any, Callable

import pandas as pd
from hdfs import Client
from pymongo import MongoClient

READER: dict[str, Callable[[Any], pd.DataFrame]] = {
    ".csv": pd.read_csv,
    ".json": pd.read_json,
}


def mongoimport(
    client_hdfs: Client,
    hdfs_file: str,
    db_name: str,
    coll_name: str,
    db_url: str = "localhost",
    db_port: int = 27017,
):
    """
    Load a file from HDFS to a MongoDB collection

    returns: count of the documents in the new collection
    """
    hdfs_file = Path(hdfs_file)
    with MongoClient(db_url, db_port) as mongo_client:
        db = mongo_client[db_name]
        coll = db[coll_name]

        with client_hdfs.read(hdfs_file) as reader:
            df = READER[hdfs_file.suffix.lower()](reader)

        coll.delete_many({})
        if df.empty:
            return 0
        coll.insert_many(df.to_dict("records"))

        return coll.count_documents({})
