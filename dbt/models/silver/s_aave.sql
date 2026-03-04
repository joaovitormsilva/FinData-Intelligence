WITH aave as (
    SELECT *
    FROM {{ref('stg_null_casting')}}
    WHERE dgt_crrnc_cd = 'AAVE'
),
var_preco as (
    SELECT *,
    (close - open) as var_dia,
    (high - low) as var_max_dia
    FROM aave
)

SELECT * from var_preco