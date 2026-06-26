-- Teste: CardCode em invoices deve existir em business_partners
-- Verifica integridade referencial entre fato e dimensão
SELECT DISTINCT i.CardCode
FROM "awsdatacatalog"."gpcorp_silver"."invoices" i
LEFT JOIN "awsdatacatalog"."gpcorp_silver"."business_partners" bp
    ON i.CardCode = bp.CardCode
WHERE bp.CardCode IS NULL
  AND i.CardCode IS NOT NULL
LIMIT 20