-- Teste: SalesPersonCode não deve ser -1 em orders (sentinela filtrado na Silver)
SELECT DocEntry, LineNum, SalesPersonCode
FROM {{ source('gpcorp_silver', 'orders') }}
WHERE SalesPersonCode < 0
LIMIT 10
