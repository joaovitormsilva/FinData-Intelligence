import logging
from datetime import datetime
from sqlalchemy import create_engine,text


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
    