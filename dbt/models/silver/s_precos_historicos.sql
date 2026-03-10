WITH aave as (
    SELECT *
    FROM {{ref('precos_historicos')}}
),
var_preco as (
    SELECT *,
    (close - open) as var_dia,
    (high - low) as var_max_dia
    FROM aave
)

SELECT * from var_preco