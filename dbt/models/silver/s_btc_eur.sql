SELECT
    timestamp AS date,
    open,
    high,
    low,
    close,
    'BTC'AS dgt_crrnc_cd,
    'EUR' AS mrkt_cd
FROM {{ source('stg_btc_eur_table', 'currency_daily_btc_eur') }}


