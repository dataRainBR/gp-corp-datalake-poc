-- Teste: SalesPersonCode não deve ser negativo em invoices
-- Exceção: -1 é sentinela válido do SAP B1 para "sem vendedor atribuído"
-- (e-commerce, venda direta, NF de bonificação). Aceito como válido.
SELECT DocEntry, LineNum, SalesPersonCode
FROM {{ source('gpcorp_silver', 'invoices') }}
WHERE SalesPersonCode < -1
LIMIT 10
