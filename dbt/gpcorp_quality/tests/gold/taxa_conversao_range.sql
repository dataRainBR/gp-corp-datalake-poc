-- Teste: taxa_conversao deve estar entre 0 e 1 (ou 0 e 100% se >1 por múltiplos pedidos)
-- Valores negativos indicam erro de cálculo
SELECT ano, mes, cod_vendedor, cod_item, taxa_conversao
FROM {{ source('gpcorp_gold_cotacoes', 'taxa_conversao') }}
WHERE taxa_conversao < 0
LIMIT 10
