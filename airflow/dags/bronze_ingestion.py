import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@task()
def extract():
    from ingestion.crypto_api import crypto

    dicionario = {'Meta Data': 
                {'1. Information': 
                 'Daily Prices and Volumes for Digital Currency', '2. Digital Currency Code': 'AAVE', '3. Digital Currency Name': 'Aave', '4. Market Code': 'GBP', '5. Market Name': 'British Pound Sterling', '6. Last Refreshed': '2026-02-12 00:00:00', '7. Time Zone': 'UTC'}, 'Time Series (Digital Currency Daily)':  
                 {'2026-02-12': {'1. open': '79.16000000', '2. high': '81.00000000', '3. low': '79.16000000', '4. close': '80.13000000', '5. volume': '380.28900000'}, 
                  '2026-02-11': {'1. open': '80.46000000', '2. high': '83.21000000', '3. low': '76.45000000', '4. close': '79.04000000', '5. volume': '1130.86200000'}, 
                   '2026-02-10': {'1. open': '82.13000000', '2. high': '82.70000000', '3. low': '79.00000000', '4. close': '80.20000000', '5. volume': '811.77700000'}, 
                   '2026-02-09': {'1. open': '82.72000000', '2. high': '84.38000000', '3. low': '80.10000000', '4. close': '82.26000000', '5. volume': '912.00900000'}, 
                   '2026-02-08': {'1. open': '83.67000000', '2. high': '84.67000000', '3. low': '82.14000000', '4. close': '82.82000000', '5. volume': '1467.72200000'}, 
                 }
            }
    
    # dicionario = crypto(function="DIGITAL_CURRENCY_DAILY", digital_currency_code="AAVE", market_code="GBP")

    return dicionario

@task()
def transform(dicionario):
    from ingestion.tb_create import table_create

    dic_transformado = table_create(dicionario)
    return dic_transformado

@task()
def load_stg(dicionario):
    from ingestion.write_stg_table import write_stg_table
    from ingestion.connect_db import connect_pg

    connection = connect_pg()

    write_stg_table(connection, dicionario, "stg_precos_historicos_ativos_crypto")


@task()
def load_silver():
    from ingestion.connect_db import connect_pg
    from ingestion.call_procedure import call_procedure

    connection = connect_pg()

    call_procedure(connection)

@task()
def read_table():
    from ingestion.connect_db import connect_pg
    from ingestion.read_db import read_from_db
    
    connection = connect_pg()
    query = "SELECT * FROM bronze.precos_historicos_ativos_crypto LIMIT 5"

    data = read_from_db(connection, query)
    logging.info('Tabela no banco:')
    logging.info(data)

@dag(
    dag_id='bronze_ingestion',
    schedule='@Daily',
    catchup=False,
    tags=["bronze_data_ingestion"],
)
def teste_pipe():
    # Definindo o fluxo
    trigger_next_dag = TriggerDagRunOperator(
        task_id = "trigger_next_dag",
        trigger_dag_id = "dbt_transform",
        wait_for_completion=True
    )

    load_stg(transform(extract())) >> load_silver() >> read_table() >> trigger_next_dag 


# Instanciação explícita
dag_obj = teste_pipe()