import csv
import logging

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