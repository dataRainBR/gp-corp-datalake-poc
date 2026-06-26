-- Teste: PK (doc_entry + num_linha) deve ser única na vendas_detalhada
SELECT doc_entry, num_linha, COUNT(*) as qtd
FROM "awsdatacatalog"."gpcorp_gold_vendas"."vendas_detalhada"
GROUP BY doc_entry, num_linha
HAVING COUNT(*) > 1