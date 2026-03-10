import logging
from sqlalchemy import text


def call_procedure(engine):
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        try:
            logging.info("Chamando procedure de carga bronze...")
            connection.execute(text("CALL bronze.sp_load_historico_crypto(30)"))
            logging.info("Procedure finalizada com sucesso!")
        except Exception as e:
            logging.error(f"Erro na execução da procedure: {e}")
            raise 