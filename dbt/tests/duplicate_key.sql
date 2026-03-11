SELECT 
    date,
    dgt_crrnc_cd,
    mrkt_cd
FROM {{ ref('s_precos_historicos')}}
GROUP BY 1,2,3
HAVING count(*) > 1