CREATE TABLE IF NOT EXISTS precos_historicos_ativos_crypto (
    date VARCHAR(10),
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    dgt_crrnc_cd VARCHAR(10),
    mrkt_cd VARCHAR(10),
    CONSTRAINT key_prmry PRIMARY KEY (date, dgt_crrnc_cd,mrkt_cd)
);