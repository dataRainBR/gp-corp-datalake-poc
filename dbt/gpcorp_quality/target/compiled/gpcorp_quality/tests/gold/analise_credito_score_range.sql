-- Teste: score_credito deve estar entre 0 e 1000 (range típico Serasa/Boa Vista)
SELECT cod_cliente, nome_cliente, score_credito
FROM "awsdatacatalog"."gpcorp_gold_cadastros"."analise_credito"
WHERE score_credito IS NOT NULL
  AND (score_credito < 0 OR score_credito > 1000)
LIMIT 10