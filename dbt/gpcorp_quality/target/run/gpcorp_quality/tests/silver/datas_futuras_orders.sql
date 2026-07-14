
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: DocDate em orders não pode ser no futuro
SELECT DocEntry, LineNum, DocDate
FROM "awsdatacatalog"."gpcorp_silver"."orders"
WHERE DocDate > current_date
LIMIT 10
  
  
      
    ) dbt_internal_test