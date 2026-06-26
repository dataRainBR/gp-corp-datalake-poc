-- Teste: PK composta (DocEntry + LineNum) única em invoices
SELECT DocEntry, LineNum, COUNT(*) as qtd
FROM "awsdatacatalog"."gpcorp_silver"."invoices"
GROUP BY DocEntry, LineNum
HAVING COUNT(*) > 1