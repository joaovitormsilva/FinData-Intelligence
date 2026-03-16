WITH aave_positive_days as (
    SELECT date,
        var_dia,
        var_max_dia
    FROM {{ ref('s_precos_historicos') }}
    WHERE dgt_crrnc_cd = 'AAVE' AND
            var_dia > 0
)

SELECT * FROM aave_positive_days

