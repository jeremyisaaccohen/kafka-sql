import yfinance as yf

def get_stock_price(ticker):
    stock = yf.Ticker(ticker)
    price = stock.history(period='1d')['Close'][0]
    return price

# Example usage
ticker = 'AAPL'
price = get_stock_price(ticker)
print(f'The current price of {ticker} is ${price}')
