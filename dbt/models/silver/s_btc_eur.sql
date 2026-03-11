WITH btc_eur AS(
    SELECT *
    FROM {{ ref('btc_eur') }}
),
btc_eur_preco AS(
    SELECT *,
    (close - open) as var_dia,
    (high - low) as var_max_dia
    FROM btc_eur
)

SELECT * FROM btc_eur_preco
