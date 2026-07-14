-- Teste: valor_linha e quantidade devem ser coerentes na vendas_detalhada
-- quantidade > 0 implica valor_linha > 0 (exceto brindes FreeOfCharge)
SELECT num_doc, num_linha, quantidade, valor_linha, preco_unitario
FROM {{ source('gpcorp_gold_vendas', 'vendas_detalhada') }}
WHERE quantidade > 0 AND valor_linha <= 0 AND preco_unitario > 0
LIMIT 10
