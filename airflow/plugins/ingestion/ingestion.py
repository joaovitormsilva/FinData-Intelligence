import os
import logging 
import requests
import pandas as pd # Adicionado
from datetime import datetime # Adicionado
from sqlalchemy import create_engine, text

# Configuração básica de logging para saída no console
logging.basicConfig(level=logging.INFO)

table = {'Meta Data': 
            {'1. Information': 
                'Daily Prices and Volumes for Digital Currency', '2. Digital Currency Code': 'AAVE', '3. Digital Currency Name': 'Aave', '4. Market Code': 'GBP', '5. Market Name': 'British Pound Sterling', '6. Last Refreshed': '2026-02-12 00:00:00', '7. Time Zone': 'UTC'}, 
                'Time Series (Digital Currency Daily)':  
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
    host = os.getenv('POSTGRES_HOST')

    conn = f"postgresql+psycopg2://{user}:{password}@{host}/{db}"

    engine = create_engine(conn)

    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            logging.info("Conexão bem-sucedida ao banco de dados PostgreSQL!")
        return engine
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        return AssertionError(f"Error to connect with database {e}")

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

def write_stg_table(engine, dicionario, stg_table_name, ):
    cols_list = list(dicionario[0].keys())
    columns = ", ".join(cols_list)

    with engine.connect() as connection:
        #Inserindo na tabela stg
        query_stg =text(f"""INSERT INTO bronze.{stg_table_name} ({columns})
        VALUES (CAST(:date AS DATE), :open, :high, :low, :close, :volume, :dgt_crrnc_cd, :mrkt_cd)
        ON CONFLICT DO NOTHING;
        """)
        try:
            connection.execute(query_stg, dicionario)
            connection.commit()
        except Exception as e:
            logging.error(f'Erro ao inserir na tabela staging: {e}')
            return AssertionError(f'Erro ao inserir os dados na tabela staging')
        

def read_from_db(engine, query):

    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            data = result.fetchall()
            logging.info(f"Consulte was sucess in {datetime.now()} and result was {data}!")
            return data
    except Exception as e:
        logging.error(f"Erro ao executar consulta: {e}")
        return AssertionError(f'Error to read table from database in read_from_db')
    
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
    
def call_procedure(engine):
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        try:
            logging.info("Chamando procedure de carga bronze...")
            connection.execute(text("CALL bronze.sp_load_historico_cryto(30)"))
            logging.info("Procedure finalizada com sucesso!")
        except Exception as e:
            logging.error(f"Erro na execução da procedure: {e}")
            raise # Levanta o erro para o Airflow saber que falhou

# --- Função Main para execução na ordem correta ---
def main():
    # 1. Criar DataFrame a partir do dicionário
    dicionario = table_create(table)
    # 2. Conectar ao PostgreSQL
    engine = connect_pg()
    
    # 3. Se a conexão for bem sucedida, escrever e ler
    if engine and not isinstance(engine, AssertionError):
        # escrevendo na stg
        write_stg_table(
            engine=engine,
            dicionario=dicionario,
            stg_table_name="stg_precos_historicos_ativos_crypto",
            )
        read_from_db(
            engine=engine, 
            query="SELECT * FROM bronze.stg_precos_historicos_ativos_crypto LIMIT 5"
        )
       
        call_procedure(engine)
        
        # Leitura para teste
        read_from_db(
            engine=engine, 
            query="SELECT * FROM bronze.precos_historicos_ativos_crypto LIMIT 5"
        )
    else:
        logging.error("Não foi possível prosseguir: Falha na conexão com o banco.")

if __name__ == "__main__":
    main()