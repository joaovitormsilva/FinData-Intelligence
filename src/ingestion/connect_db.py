import os
import logging 
from sqlalchemy import create_engine, text

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
        raise ConnectionError(f"Error to connect with database {e}")
        
