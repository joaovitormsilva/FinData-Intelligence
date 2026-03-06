import logging
from datetime import datetime
from sqlalchemy import create_engine,text

def write_to_db(engine, df, table_name, key_columns=None):
    stg_table = f'stg_{table_name}'

    with engine.connect() as connection:
        #criando a tabela temporaria
        try:
            df.to_sql(stg_table,con=connection, schema='bronze',if_exists ='replace', method='multi', index=False)
            logging.info(f'Stg_table was create sucessful in {datetime.now()}')
        except Exception as e:
            logging.error(f'Error in stg table creation, {e}')
            return AssertionError(f'Error in stg table creation in write_to_db')
        
        key_columns=", ".join(key_columns)
        columns=", ".join(df.columns) if key_columns else ""

        #Inserindo na tabela final
        query = f"""INSERT INTO bronze.{table_name} ({columns}) 
        SELECT {columns} FROM bronze.{stg_table}
        ON CONFLICT ({key_columns}) DO NOTHING;"""

        try:
            connection.execute(text(query))
            # connection.commit()
            logging.info(f'Data was persisted sucessful, the query insert was {text(query)}')
        finally:
            query = f'DROP TABLE IF EXISTS bronze.{stg_table}'
            connection.execute(text(query))
            # connection.commit()
            logging.info(f'Tabela temporaria {stg_table} dropada')
