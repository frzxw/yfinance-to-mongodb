# yfinance to MongoDB

## Overview
This project fetches financial data from Yahoo Finance and stores it in a MongoDB database. It extracts ticker symbols from a CSV file, retrieves various types of financial data, and organizes it for storage.

## Features
- Extracts ticker symbols from a CSV file.
- Fetches stock information, historical data, intraday data, financials, dividends, splits, and news articles.
- Stores the fetched data in a MongoDB database.
- Utilizes environment variables for configuration.

## Requirements
- Python 3.x
- MongoDB
- Required Python packages listed in `requirements.txt`

## Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd yfinance-to-mongodb
   ```

2. **Create a virtual environment (optional but recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install the required packages:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables:**
   Create a `.env` file in the root directory with the following content:
   ```
   MONGO_URI=mongodb://localhost:27017/
   DB_NAME=idx_yfinance_data
   CSV_FILE_PATH=data/idx.csv
   ```

5. **Prepare your CSV file:**
   Ensure that the `data/idx.csv` file contains the ticker codes you want to process.

## Usage
To run the application, execute the following command:
```bash
python run.py
```

This will extract tickers from the specified CSV file, fetch the corresponding financial data from Yahoo Finance, and store it in the MongoDB database.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.