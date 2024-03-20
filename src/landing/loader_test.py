from pymongo import MongoClient
from pathlib import Path
import pandas as pd
import json


def load_csv(file: Path):
    data = pd.read_csv(file)
    return json.loads(data.to_json(orient="records"))


def load_json(file: Path):
    with open(
        file,
        mode="r",
        encoding="utf-8",
    ) as handler:
        return json.load(handler)


def insert(
    payload,
    db_name: str,
    coll_name: str,
    db_url: str = "localhost",
    db_port: int = 27017,
):
    with MongoClient(db_url, db_port) as client:
        db = client[db_name]
        coll = db[coll_name]
        coll.delete_many({})
        coll.insert_many(payload)


LOADERS = {
    ".csv": load_csv,
    ".json": load_json,
}


def load(source: str, version: str, file: str):
    file = Path(file)
    ext = file.suffix.lower()
    insert(
        LOADERS[ext](file),
        source,
        version,
    )
