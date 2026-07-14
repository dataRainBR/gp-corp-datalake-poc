
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: DocDate em invoices não pode ser no futuro
-- Se retornar registros, há dados com data inválida
SELECT DocEntry, LineNum, DocDate
FROM "awsdatacatalog"."gpcorp_silver"."invoices"
WHERE DocDate > current_date
LIMIT 10
  
  
      
    ) dbt_internal_test