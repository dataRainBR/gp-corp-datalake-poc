-- Teste: Partições year/month devem ter valores válidos (não nulos, dentro do range)
-- Checa invoices como representante de fatos particionadas

SELECT DocEntry, LineNum, year, month
FROM "awsdatacatalog"."gpcorp_silver"."invoices"
WHERE year IS NULL
   OR month IS NULL
   OR year < 2000
   OR year > 2030
   OR month < 1
   OR month > 12
LIMIT 10