-- Teste: SalesPersonCode não deve ser negativo em quotations
-- Exceção: -1 é sentinela válido do SAP B1 para "sem vendedor atribuído"
SELECT DocEntry, LineNum, SalesPersonCode
FROM {{ source('gpcorp_silver', 'quotations') }}
WHERE SalesPersonCode < -1
LIMIT 10
