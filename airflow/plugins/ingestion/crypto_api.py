import os
import logging
import requests

def crypto(function, digital_currency_code, market_code):
    key = os.getenv("ALPHA_VANTAGE_API_KEY")
   
    url = "https://www.alphavantage.co/query"
   
    params = {
        "function": function,
        "symbol": digital_currency_code,
        "market": market_code,
        "apikey": key
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        print(data)
        return data
    else:
        logging.error(f"Failed to fetch data: {response.status_code}")
