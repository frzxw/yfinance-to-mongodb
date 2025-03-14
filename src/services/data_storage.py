from pymongo import MongoClient
from bson import json_util
import logging
from ..config import MONGO_URI, DB_NAME

def store_to_mongodb(ticker, data):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    try:
        for collection, records in data.items():
            if not records:
                continue
            
            collection_obj = db[collection]
            if collection == "stocks":
                # Ensure ticker field position and upsert the document
                serialized_data = json.loads(json_util.dumps(records))
                collection_obj.update_one(
                    {"ticker": ticker},
                    {"$set": serialized_data},
                    upsert=True
                )
            else:
                # For list-type data (historical, news, etc.)
                collection_obj.delete_many({"ticker": ticker})
                serialized_data = json.loads(json_util.dumps(records))
                collection_obj.insert_many(serialized_data)
        
        logging.info(f"Successfully stored all data for {ticker}")
    except Exception as e:
        logging.error(f"Error storing data for {ticker}: {e}")
    finally:
        client.close()