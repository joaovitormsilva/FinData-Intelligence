WITH aave_mvg_avg as (
    SELECT
        date,
        open,
        high,
        low,
        close,
        volume,
        dgt_crrnc_cd,
        mrkt_cd,
        var_dia,
        var_max_dia,
        avg(open) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as open_avg,
        avg(close) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as close_avg
    FROM {{ ref('s_precos_historicos') }}
    WHERE dgt_crrnc_cd = 'AAVE'
)

SELECT * FROM aave_mvg_avg