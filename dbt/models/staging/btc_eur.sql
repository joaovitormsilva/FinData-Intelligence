WITH testando as (
    SELECT 
        timestamp AS date,
        open,
        high,
        low,
        close,
        volume,
        'BTC' AS dgt_crrnc_cd,
        'EUR' AS mrkt_cd
    FROM {{ source('btc_eur_stg', 'currency_daily_btc_eur')}}
),

null_clean as (
    SELECT *
    FROM testando
    WHERE 
        date IS NOT NULL AND
        open IS NOT NULL AND
        high IS NOT NULL AND
        low IS NOT NULL AND
        close IS NOT NULL AND
        volume IS NOT NULL AND
        dgt_crrnc_cd IS NOT NULL AND
        mrkt_cd IS NOT NULL
),

casting as (
    SELECT 
        CAST(date as TIMESTAMP) as date, 
        open,
        high,
        low,
        close,
        volume,
        dgt_crrnc_cd,
        mrkt_cd 
     from null_clean
)

SELECT * FROM casting