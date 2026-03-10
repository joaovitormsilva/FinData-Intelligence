import logging
from datetime import datetime
from sqlalchemy import create_engine,text

def write_stg_table(engine, dicionario, stg_table_name):

    if dicionario is None:
        raise Exception('Dicionario eh nulo')
    

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT", preserve_rowcount=True) as connection:
        #Inserindo na tabela stg
        query_stg =text(f"""INSERT INTO bronze.{stg_table_name} (date, open, high, low, close, volume, dgt_crrnc_cd, mrkt_cd)
        VALUES (CAST(:date AS DATE), :open, :high, :low, :close, :volume, :dgt_crrnc_cd, :mrkt_cd)
        ON CONFLICT DO NOTHING;
        """)
        try:
            result = connection.execute(query_stg, dicionario)
            affected = result.rowcount
            logging.info(f'number of rows affetec: {affected}')
            if affected == 0:
                logging.warning('0 linhas foram inseridas')
        except Exception as e:
            logging.error(f'Erro ao inserir na tabela staging: {e}')
            raise Exception(f'Erro ao inserir os dados na tabela staging')
        
