-- Teste: SalesPersonCode não deve ser -1 em invoices (sentinela filtrado na Silver)
SELECT DocEntry, LineNum, SalesPersonCode
FROM {{ source('gpcorp_silver', 'invoices') }}
WHERE SalesPersonCode < 0
LIMIT 10
