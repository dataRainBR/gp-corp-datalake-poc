-- Teste: tabelas devem ter quantidade mínima de registros (evita carga vazia)
-- Thresholds baseados no volume conhecido

WITH contagens AS (
    SELECT 'business_partners' as tabela, COUNT(*) as qtd FROM "awsdatacatalog"."gpcorp_silver"."business_partners" WHERE _is_current = true
    UNION ALL
    SELECT 'items', COUNT(*) FROM "awsdatacatalog"."gpcorp_silver"."items" WHERE _is_current = true
    UNION ALL
    SELECT 'sales_persons', COUNT(*) FROM "awsdatacatalog"."gpcorp_silver"."sales_persons" WHERE _is_current = true
    UNION ALL
    SELECT 'quotations', COUNT(*) FROM "awsdatacatalog"."gpcorp_silver"."quotations"
)
SELECT tabela, qtd
FROM contagens
WHERE (tabela = 'business_partners' AND qtd < 10000)
   OR (tabela = 'items' AND qtd < 2000)
   OR (tabela = 'sales_persons' AND qtd < 100)
   OR (tabela = 'quotations' AND qtd < 5000)