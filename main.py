import os
import logging
from src.config import MONGO_URI, DB_NAME, CSV_FILE_PATH
from src.data.ticker_extractor import extract_tickers
from src.services.data_fetcher import fetch_ticker_data
from src.services.data_storage import store_to_mongodb

def main():
    tickers = extract_tickers(CSV_FILE_PATH)
    for ticker in tickers:
        data = fetch_ticker_data(ticker)
        store_to_mongodb(ticker, data)

if __name__ == "__main__":
    main()