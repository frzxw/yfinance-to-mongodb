import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "idx_yfinance_data")
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "data/idx.csv")