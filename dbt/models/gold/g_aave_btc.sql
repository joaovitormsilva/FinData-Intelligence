WITH related_price AS(
    SELECT A.date,
        A.open AS aave_open,
        A.close AS aave_close,
        ((A.close - A.open)/ NULLIF(A.open,0)) * 100 AS aave_per_var,
        B.open AS btc_open,
        B.close AS btc_close,
        ((B.close - B.open)/ NULLIF(B.open,0)) * 100 AS btc_per_var
    FROM {{ ref('s_precos_historicos') }} A
    JOIN {{ ref('s_btc_eur') }} B on A.date = B.date
)

SELECT * FROM related_price