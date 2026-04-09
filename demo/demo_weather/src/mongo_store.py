from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from pymongo import MongoClient

from .config import MONGO_COLLECTION_NAME, MONGO_DB_NAME, MONGO_URI


def get_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    return client, db[MONGO_COLLECTION_NAME]


def save_city_weather(record: Dict[str, Any]) -> None:
    client, collection = get_collection()
    try:
        record["stored_at_utc"] = datetime.now(timezone.utc)
        collection.update_one(
            {
                "city": record["city"],
                "source_type": record["source_type"],
            },
            {"$set": record},
            upsert=True,
        )
    finally:
        client.close()