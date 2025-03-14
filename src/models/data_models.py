from collections import OrderedDict

class StockData:
    def __init__(self, ticker, last_updated, info):
        self.ticker = ticker
        self.last_updated = last_updated
        self.info = info

class HistoricalData:
    def __init__(self, ticker, data):
        self.ticker = ticker
        self.data = data

class IntradayData:
    def __init__(self, ticker, data):
        self.ticker = ticker
        self.data = data

class FinancialData:
    def __init__(self, ticker, data):
        self.ticker = ticker
        self.data = data

class DividendData:
    def __init__(self, ticker, date, dividend):
        self.ticker = ticker
        self.date = date
        self.dividend = dividend

class SplitData:
    def __init__(self, ticker, date, split):
        self.ticker = ticker
        self.date = date
        self.split = split

def ensure_ticker_position(data, ticker):
    if isinstance(data, dict):
        result = OrderedDict()
        result['ticker'] = ticker
        for key, value in data.items():
            if key != 'ticker':
                result[key] = value
        return dict(result)
    elif isinstance(data, list):
        return [ensure_ticker_position(item, ticker) for item in data]
    return data