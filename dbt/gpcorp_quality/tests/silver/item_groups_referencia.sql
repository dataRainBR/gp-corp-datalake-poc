-- Teste: ItemsGroupCode em items deve existir em item_groups
SELECT DISTINCT i.ItemsGroupCode
FROM {{ source('gpcorp_silver', 'items') }} i
LEFT JOIN {{ source('gpcorp_silver', 'item_groups') }} ig
    ON i.ItemsGroupCode = ig.Number
WHERE ig.Number IS NULL
  AND i.ItemsGroupCode IS NOT NULL
  AND i._is_current = true
LIMIT 10
