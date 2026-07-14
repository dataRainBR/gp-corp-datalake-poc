-- Teste: LineTotal em invoices deve ser >= 0
-- NFs de devolução/cancelamento usam docs separados no SAP B1
SELECT DocEntry, LineNum, LineTotal, ItemCode
FROM "awsdatacatalog"."gpcorp_silver"."invoices"
WHERE LineTotal < 0
LIMIT 10