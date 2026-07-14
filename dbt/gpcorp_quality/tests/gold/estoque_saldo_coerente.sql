-- Teste: saldo_qtd deve ser igual a qtd_entrada - qtd_saida
SELECT ano, mes, cod_item, cod_deposito,
       qtd_entrada, qtd_saida, saldo_qtd,
       (qtd_entrada - qtd_saida) as saldo_esperado
FROM {{ source('gpcorp_gold_estoque', 'movimentacao_estoque') }}
WHERE ABS(saldo_qtd - (qtd_entrada - qtd_saida)) > 0.01
LIMIT 10
