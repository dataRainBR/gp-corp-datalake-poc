-- Teste: DocDate em invoices não pode ser no futuro
-- Se retornar registros, há dados com data inválida
SELECT DocEntry, LineNum, DocDate
FROM "awsdatacatalog"."gpcorp_silver"."invoices"
WHERE DocDate > current_date
LIMIT 10