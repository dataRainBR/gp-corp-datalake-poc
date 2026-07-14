
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: LineTotal em invoices deve ser >= 0
-- NFs de devolução/cancelamento usam docs separados no SAP B1
SELECT DocEntry, LineNum, LineTotal, ItemCode
FROM "awsdatacatalog"."gpcorp_silver"."invoices"
WHERE LineTotal < 0
LIMIT 10
  
  
      
    ) dbt_internal_test