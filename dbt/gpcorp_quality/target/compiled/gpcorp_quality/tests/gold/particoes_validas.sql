-- Teste: partições ano/mes devem ser válidas em todas tabelas Gold particionadas
SELECT 'faturamento_mensal' as tabela, ano, mes
FROM "awsdatacatalog"."gpcorp_gold_vendas"."faturamento_mensal"
WHERE ano < 2000 OR ano > 2030 OR mes < 1 OR mes > 12

UNION ALL

SELECT 'ranking_vendedores' as tabela, ano, mes
FROM "awsdatacatalog"."gpcorp_gold_vendas"."ranking_vendedores"
WHERE ano < 2000 OR ano > 2030 OR mes < 1 OR mes > 12

UNION ALL

SELECT 'vendas_por_produto' as tabela, ano, mes
FROM "awsdatacatalog"."gpcorp_gold_vendas"."vendas_por_produto"
WHERE ano < 2000 OR ano > 2030 OR mes < 1 OR mes > 12