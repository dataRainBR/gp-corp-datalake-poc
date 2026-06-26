-- Teste: DocDate não deve ter datas no futuro (possível erro de carga)
-- Tolerância: até D+1 (pode ser fuso horário)

SELECT DocEntry, LineNum, DocDate
FROM "awsdatacatalog"."gpcorp_silver"."quotations"
WHERE DocDate > CURRENT_DATE + INTERVAL '1' DAY
LIMIT 10