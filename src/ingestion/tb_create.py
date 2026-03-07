import pandas as pd
import logging


def table_create(dicionario):
    rows = []
    meta = dicionario['Meta Data']
    time_series = dicionario['Time Series (Digital Currency Daily)']

    for date, values in time_series.items():
        row = {
            "date": date,
            "open": float(values['1. open']),
            "high": float(values['2. high']),
            "low": float(values['3. low']),
            "close": float(values['4. close']),
            "volume": float(values['5. volume']),
            "dgt_crrnc_cd": meta['2. Digital Currency Code'],
            "mrkt_cd": meta['4. Market Code']
        }
        rows.append(row)
    return rows
