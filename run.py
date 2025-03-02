from pymongo import MongoClient, errors
from collections import OrderedDict
from datetime import datetime
from bson import json_util
import yfinance as yf
import pandas as pd
import logging
import time
import json
import csv
import os

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "idx_yfinance_data"
CSV_FILE_PATH = "data/idx.csv"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DELAY = 1  # Delay in seconds
PERIOD = "1y"

# Define collections
COLLECTIONS = {
    "stocks": "stocks",
    "historical": "historical",
    "intraday": "intraday",
    "financials": "financials",
    "dividends": "dividends",
    "splits": "splits",
    "news": "news"
}

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

def extract_tickers(csv_file_path):
    logging.info(f"Extracting tickers from {csv_file_path}")
    tickers = []
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file, delimiter=';')
            for row in csv_reader:
                tickers.append(row['Code'])
        logging.info(f"Extracted {len(tickers)} tickers")
    except Exception as e:
        logging.error(f"Error extracting tickers: {e}")
    return tickers

def append_suffix_to_tickers(tickers, suffix=".JK"):
    logging.info(f"Appending suffix '{suffix}' to tickers")
    return [ticker + suffix for ticker in tickers]

def get_clean_ticker(ticker):
    """Remove .JK suffix from ticker for storage"""
    return ticker.replace(".JK", "")

def convert_timestamps(obj):
    """Convert pandas Timestamps and any other non-serializable objects to ISO format strings"""
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif hasattr(obj, 'to_dict'):
        return obj.to_dict()
    elif pd.isna(obj):
        return None
    return obj

def sanitize_data(data):
    """Convert DataFrame and Series to Python native types and handle NaN values"""
    if isinstance(data, pd.DataFrame):
        return json.loads(data.reset_index().to_json(orient='records', date_format='iso'))
    elif isinstance(data, pd.Series):
        return json.loads(data.to_json(orient='index', date_format='iso'))
    elif isinstance(data, dict):
        return {k: sanitize_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_data(item) for item in data]
    return convert_timestamps(data)

def ensure_ticker_position(data, ticker):
    """Ensure ticker field is positioned right after _id in the document"""
    if isinstance(data, dict):
        # Create an ordered dict with ticker first
        result = OrderedDict()
        result['ticker'] = ticker
        # Add all other fields
        for key, value in data.items():
            if key != 'ticker':  # Skip if it's the ticker field we just added
                result[key] = value
        return dict(result)  # Convert back to regular dict
    elif isinstance(data, list):
        # Apply to each item in the list
        return [ensure_ticker_position(item, ticker) for item in data]
    return data

def store_ticker_data(collection, ticker, data, client):
    """Store a single ticker's data to MongoDB"""
    db = client[DB_NAME]
    collection_obj = db[collection]
    
    try:
        if not data:
            return
            
        # Different handling for different data types
        if collection == COLLECTIONS["stocks"]:
            # For stocks, ensure ticker field position and upsert the document
            data = ensure_ticker_position(data, ticker)
            serialized_data = json.loads(json_util.dumps(data))
            collection_obj.update_one(
                {"ticker": ticker},
                {"$set": serialized_data},
                upsert=True
            )
        elif isinstance(data, list):
            # For list-type data (historical, news, etc.)
            # First delete existing data for this ticker
            collection_obj.delete_many({"ticker": ticker})
            
            # Then insert all new records
            if data:
                # Ensure ticker field position in each document
                data = [ensure_ticker_position(item, ticker) for item in data]
                serialized_data = json.loads(json_util.dumps(data))
                collection_obj.insert_many(serialized_data)
        
        logging.info(f"Successfully stored {collection} data for {ticker}")
    except Exception as e:
        logging.error(f"Error storing {collection} data for {ticker}: {e}")

def fetch_ticker_data(ticker):
    """Fetch data for a single ticker"""
    data = {}
    try:
        logging.info(f"Fetching data for {ticker}")
        stock = yf.Ticker(ticker)
        clean_ticker = get_clean_ticker(ticker)
        timestamp = datetime.now().isoformat()
        
        # Fetch stocks info (formerly metadata)
        try:
            info_data = stock.info
            data["stocks"] = {
                'ticker': clean_ticker,
                'last_updated': timestamp,
                'info': sanitize_data(info_data)
            }
        except Exception as e:
            logging.error(f"Error fetching info for {ticker}: {e}")
            data["stocks"] = {
                'ticker': clean_ticker,
                'last_updated': timestamp
            }
        
        # Fetch historical data
        try:
            historical_df = stock.history(period=PERIOD)
            if not historical_df.empty:
                data["historical"] = sanitize_data(historical_df)
        except Exception as e:
            logging.error(f"Error fetching historical data for {ticker}: {e}")
        
        # Fetch intraday data
        try:
            intraday_df = stock.history(interval="1h", period=PERIOD)
            if not intraday_df.empty:
                data["intraday"] = sanitize_data(intraday_df)
        except Exception as e:
            logging.error(f"Error fetching intraday data for {ticker}: {e}")
            
        # Fetch financials
        try:
            financials = stock.financials
            if not financials.empty:
                data["financials"] = sanitize_data(financials)
        except Exception as e:
            logging.error(f"Error fetching financials for {ticker}: {e}")
            
        # Fetch dividends
        try:
            dividends = stock.dividends
            if not dividends.empty:
                records = sanitize_data(dividends)
                if isinstance(records, dict):
                    dividend_list = []
                    for date, value in records.items():
                        dividend_list.append({
                            "ticker": clean_ticker,
                            "date": date,
                            "dividend": value
                        })
                    data["dividends"] = dividend_list
                else:
                    data["dividends"] = records
        except Exception as e:
            logging.error(f"Error fetching dividends for {ticker}: {e}")
            
        # Fetch splits
        try:
            splits = stock.splits
            if not splits.empty:
                records = sanitize_data(splits)
                if isinstance(records, dict):
                    split_list = []
                    for date, value in records.items():
                        split_list.append({
                            "ticker": clean_ticker,
                            "date": date,
                            "split": value
                        })
                    data["splits"] = split_list
                else:
                    data["splits"] = records
        except Exception as e:
            logging.error(f"Error fetching splits for {ticker}: {e}")
            
        # Fetch news
        try:
            news = stock.news
            if news:
                sanitized_news = []
                for article in news:
                    sanitized_article = {
                        "ticker": clean_ticker,
                        "title": article.get("title", ""),
                        "publisher": article.get("publisher", ""),
                        "link": article.get("link", ""),
                        "publishTime": article.get("providerPublishTime", 0),
                        "type": article.get("type", ""),
                        "relatedTickers": article.get("relatedTickers", []),
                        "thumbnail": article.get("thumbnail", {}).get("resolutions", [{}])[0].get("url", "") if article.get("thumbnail") else ""
                    }
                    sanitized_news.append(sanitized_article)
                data["news"] = sanitized_news
        except Exception as e:
            logging.error(f"Error processing news for {ticker}: {e}")
        
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
    
    return data

def store_to_mongodb(ticker, data, client):
    """Store fetched data for a single ticker to MongoDB"""
    clean_ticker = get_clean_ticker(ticker)
    try:
        for collection, records in data.items():
            store_ticker_data(COLLECTIONS[collection], clean_ticker, records, client)
        logging.info(f"Successfully stored all data for {ticker}")
    except Exception as e:
        logging.error(f"Error storing data for {ticker}: {e}")

def fetch_and_store_data(ticker, client):
    """Fetch and immediately store data for a single ticker"""
    data = fetch_ticker_data(ticker)
    store_to_mongodb(ticker, data, client)
    time.sleep(DELAY)

def main():
    # Ensure data directory exists
    os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
    
    # Extract tickers from CSV file
    tickers = extract_tickers(CSV_FILE_PATH)
    tickers = append_suffix_to_tickers(tickers)
    
    try:
        # Establish a single MongoDB connection for the entire script
        client = MongoClient(MONGO_URI)
        logging.info("Connected to MongoDB")
        
        # Process each ticker individually
        for ticker in tickers:
            fetch_and_store_data(ticker, client)
        
        logging.info("All tickers processed successfully")
        client.close()
    except errors.PyMongoError as e:
        logging.error(f"Error connecting to MongoDB: {e}")

if __name__ == "__main__":
    main()