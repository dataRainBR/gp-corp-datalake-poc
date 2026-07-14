-- Teste: CardCode em orders deve existir em business_partners
-- Severidade: warn (parceiro pode ter sido criado após última extração Bronze)
{{
  config(
    severity = 'warn'
  )
}}
SELECT DISTINCT o.CardCode
FROM {{ source('gpcorp_silver', 'orders') }} o
LEFT JOIN {{ source('gpcorp_silver', 'business_partners') }} bp
    ON o.CardCode = bp.CardCode
WHERE bp.CardCode IS NULL
  AND o.CardCode IS NOT NULL
LIMIT 20
