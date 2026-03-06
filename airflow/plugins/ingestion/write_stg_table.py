import logging
from datetime import datetime
from sqlalchemy import create_engine,text

def write_stg_table(engine, dicionario, stg_table_name):
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
        
