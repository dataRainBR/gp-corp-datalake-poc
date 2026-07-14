
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: ItemCode nas cotações deve existir na dimensão items
-- Retorna itens órfãos (FK quebrada)

SELECT DISTINCT q.ItemCode
FROM "awsdatacatalog"."gpcorp_silver"."quotations" q
LEFT JOIN "awsdatacatalog"."gpcorp_silver"."items" i
    ON q.ItemCode = i.ItemCode AND i._is_current = true
WHERE i.ItemCode IS NULL
    AND q.ItemCode IS NOT NULL
  
  
      
    ) dbt_internal_test