from typing import Tuple, List, Any

import yfinance as yf

msft = yf.Ticker("MSFT")

def stock_info(ticker: str) -> tuple[list[str], list[Any]]:
    stock = yf.Ticker(ticker)
    fast_info = stock.fast_info
    return fast_info.keys(), fast_info.values()

print(type(msft.fast_info.values()[11]))
