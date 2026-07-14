-- Teste: CardCode em invoices deve existir em business_partners
-- Verifica integridade referencial entre fato e dimensão
-- Severidade: warn (1 registro órfão é aceitável — pode ser parceiro
-- criado após a última extração Bronze ou inativado/excluído no SAP)
{{
  config(
    severity = 'warn'
  )
}}
SELECT DISTINCT i.CardCode
FROM {{ source('gpcorp_silver', 'invoices') }} i
LEFT JOIN {{ source('gpcorp_silver', 'business_partners') }} bp
    ON i.CardCode = bp.CardCode
WHERE bp.CardCode IS NULL
  AND i.CardCode IS NOT NULL
LIMIT 20
