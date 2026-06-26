-- Teste: total_pedidos nunca pode ser maior que total_cotacoes * 10
-- (margem ampla, mas impede dados absurdos)
-- E taxa_conversao deve ser consistente com total_pedidos/total_cotacoes
SELECT cod_cliente, cod_vendedor, total_cotacoes, total_pedidos, taxa_conversao
FROM {{ source('gpcorp_gold_cotacoes', 'features_predicao_conversao') }}
WHERE total_pedidos > total_cotacoes * 10
   OR (total_cotacoes > 0 AND ABS(taxa_conversao - CAST(total_pedidos AS DOUBLE) / total_cotacoes) > 0.01)
LIMIT 10
