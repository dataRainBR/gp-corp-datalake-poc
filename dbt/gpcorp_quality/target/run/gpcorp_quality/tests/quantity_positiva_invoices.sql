
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: Quantity em invoices deve ser > 0 (linhas ativas)
SELECT DocEntry, LineNum, Quantity, ItemCode
FROM "awsdatacatalog"."gpcorp_silver"."invoices"
WHERE Quantity <= 0
  AND LineStatus = 'bost_Open'
LIMIT 10
  
  
      
    ) dbt_internal_test