-- Teste: Cada SalesEmployeeCode deve ter exatamente 1 registro com _is_current = true
SELECT
    SalesEmployeeCode,
    COUNT(*) as qtd_correntes
FROM "awsdatacatalog"."gpcorp_silver"."sales_persons"
WHERE _is_current = true
GROUP BY SalesEmployeeCode
HAVING COUNT(*) > 1