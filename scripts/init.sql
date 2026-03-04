CREATE SCHEMA bronze;
CREATE SCHEMA silver;

CREATE TABLE bronze.precos_historicos_ativos_crypto(
    date VARCHAR(10),
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    dgt_crrnc_cd  VARCHAR(10),
    mrkt_cd  VARCHAR(10),
    CONSTRAINT primary_key PRIMARY KEY(date,dgt_crrnc_cd,mrkt_cd)

);
