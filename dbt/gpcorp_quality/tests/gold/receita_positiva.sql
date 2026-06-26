-- Teste: receita_total no faturamento_mensal deve ser > 0
-- Agregação de vendas não pode resultar em valor negativo
SELECT ano, mes, cod_vendedor, cod_cliente, receita_total
FROM {{ source('gpcorp_gold_vendas', 'faturamento_mensal') }}
WHERE receita_total <= 0
LIMIT 10
