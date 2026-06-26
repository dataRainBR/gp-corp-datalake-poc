-- Teste: ItemCode nas cotações deve existir na dimensão items
-- Retorna itens órfãos (FK quebrada)

SELECT DISTINCT q.ItemCode
FROM {{ source('gpcorp_silver', 'quotations') }} q
LEFT JOIN {{ source('gpcorp_silver', 'items') }} i
    ON q.ItemCode = i.ItemCode AND i._is_current = true
WHERE i.ItemCode IS NULL
    AND q.ItemCode IS NOT NULL
