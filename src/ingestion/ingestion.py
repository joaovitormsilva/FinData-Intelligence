import os
import re
import logging 
import requests
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.DEBUG)

example= {'Meta Data': 
                {'1. Information': 
                 'Daily Prices and Volumes for Digital Currency', '2. Digital Currency Code': 'AAVE', '3. Digital Currency Name': 'Aave', '4. Market Code': 'GBP', '5. Market Name': 'British Pound Sterling', '6. Last Refreshed': '2026-02-12 00:00:00', '7. Time Zone': 'UTC'}, 'Time Series (Digital Currency Daily)':  
                 {'2026-02-12': {'1. open': '79.16000000', '2. high': '81.00000000', '3. low': '79.16000000', '4. close': '80.13000000', '5. volume': '380.28900000'}, 
                  '2026-02-11': {'1. open': '80.46000000', '2. high': '83.21000000', '3. low': '76.45000000', '4. close': '79.04000000', '5. volume': '1130.86200000'}, 
                   '2026-02-10': {'1. open': '82.13000000', '2. high': '82.70000000', '3. low': '79.00000000', '4. close': '80.20000000', '5. volume': '811.77700000'}, 
                   '2026-02-09': {'1. open': '82.72000000', '2. high': '84.38000000', '3. low': '80.10000000', '4. close': '82.26000000', '5. volume': '912.00900000'}, 
                   '2026-02-08': {'1. open': '83.67000000', '2. high': '84.67000000', '3. low': '82.14000000', '4. close': '82.82000000', '5. volume': '1467.72200000'}, 
                 }
            }


def connect_pg():
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DB')
    host = "data_center"

    conn = f"postgresql+psycopg2://{user}:{password}@{host}/{db}"

    engine = create_engine(conn)

    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            logging.info("Conexão bem-sucedida ao banco de dados PostgreSQL!")
        return engine
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return None


def write_to_db(engine, df, table_name, key_columns=None):
    stg_table = f'stg_{table_name}'

    with engine.connect() as connection:
        #criando a tabela temporaria
        df.to_sql(stg_table,con=connection,if_exists ='replace', method='multi', index=False)
        
        key_columns=", ".join(key_columns)
        columns=", ".join(df.columns) if key_columns else ""
        
        query = f'CREATE TABLE IF NOT EXISTS {table_name} (LIKE {stg_table} INCLUDING ALL)'
        connection.execute(text(query))
  
        try:
            connection.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({key_columns})"))
            connection.commit()
        except Exception as e:
            connection.rollback()
            logging.info(f'Erro ao alterar a tabela: {e}')
            
        #Inserindo na tabela final
        query = f"""INSERT INTO {table_name} ({columns}) 
        SELECT {columns} FROM {stg_table}
        ON CONFLICT ({key_columns}) DO NOTHING;"""

        try:
            connection.execute(text(query))
            connection.commit()
            logging.info('Dados persistidos')
        finally:
            query = f'DROP TABLE IF EXISTS {stg_table}'
            connection.execute(text(query))
            connection.commit()
            logging.info(f'Tabela temporaria {stg_table} dropada')

        
    

def read_from_db(engine, query):

    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            data = result.fetchall()
            logging.info("Consulta executada com sucesso!")
            return data
    except Exception as e:
        logging.error(f"Erro ao executar consulta: {e}")
        return None
    

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



def dic_convert(dicionario_entrada):

    keys = list(dicionario_entrada.keys())
    meta_data = keys[0]
    time_serie = keys[1]
    dgt_crrc_cd = list(dicionario_entrada[keys[0]])[1]
    mkt_cd = list(dicionario_entrada[keys[0]])[3]
    digital_currency_data = dicionario_entrada[meta_data][dgt_crrc_cd]
    market_code = dicionario_entrada[meta_data][mkt_cd]
    
    tb_ativo = pd.DataFrame()
    try:
        for key, value in dicionario_entrada[time_serie].items():
            
            new_row = pd.DataFrame({
                'date': [key],
                'open': [float(value['1. open'])],
                'high': [float(value['2. high'])], 
                'low': [float(value['3. low'])],
                'close': [float(value['4. close'])],
                'volume': [float(value['5. volume'])]
            })
            tb_ativo = pd.concat([tb_ativo, new_row], ignore_index=True)

    except Exception as e:
        logging.error(f"Erro ao converter dicionário: {e}")
        raise e
    
    tb_ativo['dgt_crrnc_cd'] = digital_currency_data
    tb_ativo['mrkt_cd'] = market_code
    print("Tabela de Ativo:")
    print(tb_ativo.head())

    return tb_ativo 


if __name__ == "__main__":
    connection = connect_pg()

    # dif = crypto(function="DIGITAL_CURRENCY_DAILY", digital_currency_code="AAVE", market_code="GBP")
    
    dif_df = dic_convert(example)

    write_to_db(connection, dif_df, "precos_historicos_ativos_crypto",["date", "dgt_crrnc_cd", "mrkt_cd"])

    query = "SELECT * FROM precos_historicos_ativos_crypto"

    data = read_from_db(connection, query)
    print('Tabela no banco:')
    print(data)