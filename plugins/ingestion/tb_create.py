import pandas as pd
import logging


def table_create(dicionario):
    clean_dict = dicionario['Time Series (Digital Currency Daily)']
    dgt_crry_cd =  dicionario['Meta Data']['2. Digital Currency Code']
    mkt_cd = dicionario['Meta Data']['4. Market Code']
    
    tb_ativo = pd.DataFrame.from_dict(clean_dict,orient='index')
    tb_ativo = tb_ativo.rename(columns={'date':'date', '1. open':'open', '2. high':'high', '3. low':'low', '4. close':'close', '5. volume':'volume'})
    
    tb_ativo['dgt_crrnc_cd'] = dgt_crry_cd
    tb_ativo['mrkt_cd'] = mkt_cd
    
    tb_ativo.index.name = 'date'
    tb_ativo = tb_ativo.reset_index()
    
    tb_ativo[['open','high','low','close','volume']] = tb_ativo[['open','high','low','close','volume']].astype("float") 
   
    print("Tabela de Ativo:")
    print(tb_ativo.head())
    logging.info("Tabela de Ativo:")
    logging.info(tb_ativo.head())
    return tb_ativo

