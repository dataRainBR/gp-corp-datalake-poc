-- Teste: ranking deve começar em 1 para cada mês
-- dense_rank deve gerar sequência a partir de 1
SELECT ano, mes, MIN(ranking) as min_ranking
FROM {{ source('gpcorp_gold_vendas', 'ranking_vendedores') }}
GROUP BY ano, mes
HAVING MIN(ranking) != 1
