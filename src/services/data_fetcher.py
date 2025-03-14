import yfinance as yf
import logging
from ..models.data_models import ensure_ticker_position
from ..utils.data_sanitizer import sanitize_data

def fetch_ticker_data(ticker):
    data = {}
    try:
        logging.info(f"Fetching data for {ticker}")
        stock = yf.Ticker(ticker)
        timestamp = datetime.now().isoformat()
        
        # Fetch stocks info
        try:
            info_data = stock.info
            data["stocks"] = {
                'ticker': ticker,
                'last_updated': timestamp,
                'info': sanitize_data(info_data)
            }
        except Exception as e:
            logging.error(f"Error fetching info for {ticker}: {e}")
            data["stocks"] = {
                'ticker': ticker,
                'last_updated': timestamp
            }
        
        # Fetch historical data
        try:
            historical_df = stock.history(period="1y")
            if not historical_df.empty:
                data["historical"] = sanitize_data(historical_df)
        except Exception as e:
            logging.error(f"Error fetching historical data for {ticker}: {e}")
        
        # Fetch intraday data
        try:
            intraday_df = stock.history(interval="1h", period="1y")
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
                            "ticker": ticker,
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
                            "ticker": ticker,
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
                        "ticker": ticker,
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