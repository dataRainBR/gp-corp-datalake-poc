
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: ItemsGroupCode em items deve existir em item_groups
SELECT DISTINCT i.ItemsGroupCode
FROM "awsdatacatalog"."gpcorp_silver"."items" i
LEFT JOIN "awsdatacatalog"."gpcorp_silver"."item_groups" ig
    ON i.ItemsGroupCode = ig.Number
WHERE ig.Number IS NULL
  AND i.ItemsGroupCode IS NOT NULL
  AND i._is_current = true
LIMIT 10
  
  
      
    ) dbt_internal_test