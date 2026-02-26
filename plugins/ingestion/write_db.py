import logging
from sqlalchemy import create_engine,text

def write_to_db(engine, df, table_name, key_columns=None):
    stg_table = f'stg_{table_name}'

    with engine.connect() as connection:
        #criando a tabela temporaria
        df.to_sql(stg_table,con=connection,if_exists ='replace', method='multi', index=False)
        
        key_columns=", ".join(key_columns)
        columns=", ".join(df.columns) if key_columns else ""

        #Inserindo na tabela final
        query = f"""INSERT INTO {table_name} ({columns}) 
        SELECT {columns} FROM {stg_table}
        ON CONFLICT ({key_columns}) DO NOTHING;"""

        try:
            connection.execute(text(query))
            # connection.commit()
            logging.info('Dados persistidos')
        finally:
            query = f'DROP TABLE IF EXISTS {stg_table}'
            connection.execute(text(query))
            # connection.commit()
            logging.info(f'Tabela temporaria {stg_table} dropada')
