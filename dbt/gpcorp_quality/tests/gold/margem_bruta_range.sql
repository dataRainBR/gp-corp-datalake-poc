-- Teste: pct_margem_bruta deve estar em range razoável (-100% a 100%)
-- Valores fora indicam erro de cálculo
SELECT ano, mes, cod_vendedor, nome_vendedor, pct_margem_bruta
FROM {{ source('gpcorp_gold_vendas', 'ranking_vendedores') }}
WHERE pct_margem_bruta < -100 OR pct_margem_bruta > 100
LIMIT 10
