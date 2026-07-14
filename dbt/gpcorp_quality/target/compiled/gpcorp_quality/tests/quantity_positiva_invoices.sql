-- Teste: Quantity em invoices deve ser > 0 (linhas ativas)
SELECT DocEntry, LineNum, Quantity, ItemCode
FROM "awsdatacatalog"."gpcorp_silver"."invoices"
WHERE Quantity <= 0
  AND LineStatus = 'bost_Open'
LIMIT 10