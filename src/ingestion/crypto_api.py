import os
import logging
import requests

from datetime import datetime

def crypto(function, digital_currency_code, market_code):
   
    try:
        key = os.getenv("ALPHA_VANTAGE_API_KEY")
        if key is None:
            raise Exception('Key da API Alpha Vantage é nula')
    except Exception as e:
        logging.error(f"Erro ao pegar a chave da API: {e}")
        return AssertionError(f'Error in get API Key {e}')

    url = "https://www.alphavantage.co/query"
   
    params = {
        "function": function,
        "symbol": digital_currency_code,
        "market": market_code,
        "apikey": key
    }

    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        logging.info(f'A chave foi usada em {datetime.now()}, no endpoint:{url} e o resultado foi {response.json()}')
        data = response.json()
        return data
    else:
        logging.error(f"Failed in crypto_api function, the response in request was: {response.status_code}")
        return AssertionError(f"Error in crypto_api function, key was use in {datetime.now()}, in endpoint {url} and result was {response}")