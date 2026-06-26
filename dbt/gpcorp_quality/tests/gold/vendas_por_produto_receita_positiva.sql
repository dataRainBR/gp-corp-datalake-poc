-- Teste: receita por produto deve ser > 0 (agregação de vendas)
SELECT ano, mes, cod_item, nome_item, receita
FROM {{ source('gpcorp_gold_vendas', 'vendas_por_produto') }}
WHERE receita <= 0
LIMIT 10
