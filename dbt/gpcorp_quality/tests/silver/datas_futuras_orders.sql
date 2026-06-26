-- Teste: DocDate em orders não pode ser no futuro
SELECT DocEntry, LineNum, DocDate
FROM {{ source('gpcorp_silver', 'orders') }}
WHERE DocDate > current_date
LIMIT 10
