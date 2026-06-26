-- Teste: PK (num_doc + num_linha) deve ser única na vendas_detalhada
SELECT num_doc, num_linha, COUNT(*) as qtd
FROM {{ source('gpcorp_gold_vendas', 'vendas_detalhada') }}
GROUP BY num_doc, num_linha
HAVING COUNT(*) > 1
