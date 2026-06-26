-- Teste: SalesPersonCode não deve ser -1 nas fatos (sentinela removido na Silver)
-- Se retornar registros, o filtro de negativos falhou

SELECT DocEntry, LineNum, SalesPersonCode
FROM {{ source('gpcorp_silver', 'quotations') }}
WHERE SalesPersonCode < 0
LIMIT 10
