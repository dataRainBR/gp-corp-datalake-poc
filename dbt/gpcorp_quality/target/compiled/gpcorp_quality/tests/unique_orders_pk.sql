-- Teste: PK composta (DocEntry + LineNum) única em orders
SELECT DocEntry, LineNum, COUNT(*) as qtd
FROM "awsdatacatalog"."gpcorp_silver"."orders"
GROUP BY DocEntry, LineNum
HAVING COUNT(*) > 1