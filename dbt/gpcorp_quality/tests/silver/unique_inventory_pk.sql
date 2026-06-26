-- Teste: PK composta (DocEntry + LineNum) única em inventory_gen_entries
SELECT DocEntry, LineNum, COUNT(*) as qtd
FROM {{ source('gpcorp_silver', 'inventory_gen_entries') }}
GROUP BY DocEntry, LineNum
HAVING COUNT(*) > 1
