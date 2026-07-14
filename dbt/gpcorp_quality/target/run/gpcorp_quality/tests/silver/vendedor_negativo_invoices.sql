
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: SalesPersonCode não deve ser -1 em invoices (sentinela filtrado na Silver)
SELECT DocEntry, LineNum, SalesPersonCode
FROM "awsdatacatalog"."gpcorp_silver"."invoices"
WHERE SalesPersonCode < 0
LIMIT 10
  
  
      
    ) dbt_internal_test