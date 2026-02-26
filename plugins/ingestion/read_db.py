import logging
from sqlalchemy import create_engine,text
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
    