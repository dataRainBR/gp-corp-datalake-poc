-- Teste: Cada ItemCode deve ter exatamente 1 registro com _is_current = true
SELECT
    ItemCode,
    COUNT(*) as qtd_correntes
FROM "awsdatacatalog"."gpcorp_silver"."items"
WHERE _is_current = true
GROUP BY ItemCode
HAVING COUNT(*) > 1