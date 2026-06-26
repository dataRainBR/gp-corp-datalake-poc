-- Teste: CardCode em orders deve existir em business_partners
SELECT DISTINCT o.CardCode
FROM "awsdatacatalog"."gpcorp_silver"."orders" o
LEFT JOIN "awsdatacatalog"."gpcorp_silver"."business_partners" bp
    ON o.CardCode = bp.CardCode
WHERE bp.CardCode IS NULL
  AND o.CardCode IS NOT NULL
LIMIT 20