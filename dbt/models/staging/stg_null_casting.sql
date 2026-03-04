WITH testando as (
    SELECT *
    FROM {{ source('bronze_precos_historicos_ativos_crypto', 'precos_historicos_ativos_crypto')}}
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