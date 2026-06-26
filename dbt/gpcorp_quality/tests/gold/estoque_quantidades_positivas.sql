-- Teste: qtd_entrada e qtd_saida devem ser >= 0 (já classificadas)
SELECT ano, mes, cod_item, cod_deposito, qtd_entrada, qtd_saida
FROM {{ source('gpcorp_gold_estoque', 'movimentacao_estoque') }}
WHERE qtd_entrada < 0 OR qtd_saida < 0
LIMIT 10
