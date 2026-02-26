import logging
from airflow.decorators import dag, task
from datetime import datetime

@task()
def extract():
    from crypto_api import crypto

    table = {'Meta Data': 
                {'1. Information': 
                 'Daily Prices and Volumes for Digital Currency', '2. Digital Currency Code': 'AAVE', '3. Digital Currency Name': 'Aave', '4. Market Code': 'GBP', '5. Market Name': 'British Pound Sterling', '6. Last Refreshed': '2026-02-12 00:00:00', '7. Time Zone': 'UTC'}, 'Time Series (Digital Currency Daily)':  
                 {'2026-02-12': {'1. open': '79.16000000', '2. high': '81.00000000', '3. low': '79.16000000', '4. close': '80.13000000', '5. volume': '380.28900000'}, 
                  '2026-02-11': {'1. open': '80.46000000', '2. high': '83.21000000', '3. low': '76.45000000', '4. close': '79.04000000', '5. volume': '1130.86200000'}, 
                   '2026-02-10': {'1. open': '82.13000000', '2. high': '82.70000000', '3. low': '79.00000000', '4. close': '80.20000000', '5. volume': '811.77700000'}, 
                   '2026-02-09': {'1. open': '82.72000000', '2. high': '84.38000000', '3. low': '80.10000000', '4. close': '82.26000000', '5. volume': '912.00900000'}, 
                   '2026-02-08': {'1. open': '83.67000000', '2. high': '84.67000000', '3. low': '82.14000000', '4. close': '82.82000000', '5. volume': '1467.72200000'}, 
                 }
            }
    
    # table = crypto(function="DIGITAL_CURRENCY_DAILY", digital_currency_code="AAVE", market_code="GBP")

    
    return table

@task()
def transform(table):
    from tb_create import table_create

    table = table_create(table)
    return table

@task()
def load(table):
    from write_db import write_to_db
    from connect_db import connect_pg
    connection = connect_pg()

    write_to_db(connection, table, "precos_historicos_ativos_crypto",["date", "dgt_crrnc_cd", "mrkt_cd"])
    


@task()
def read_table():
    from connect_db import connect_pg
    from read_db import read_from_db
    
    connection = connect_pg()
    query = "SELECT * FROM precos_historicos_ativos_crypto"

    data = read_from_db(connection, query)
    print('Tabela no banco:')
    print(data)

@dag(
    dag_id='ingestion_dag_01',
    schedule=None,
    start_date=datetime(2021, 1, 2),
    catchup=False,
    tags=["testando"],
)
def teste_pipe():
    # Definindo o fluxo
    table = extract()
    s_table = transform(table)
    load(s_table) >> read_table()

# Instanciação explícita
dag_obj = teste_pipe()