
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: ItemCode em invoices deve existir em items
-- Verifica integridade referencial fato→dimensão de produto
SELECT DISTINCT i.ItemCode
FROM "awsdatacatalog"."gpcorp_silver"."invoices" i
LEFT JOIN "awsdatacatalog"."gpcorp_silver"."items" it
    ON i.ItemCode = it.ItemCode
WHERE it.ItemCode IS NULL
  AND i.ItemCode IS NOT NULL
LIMIT 20
  
  
      
    ) dbt_internal_test