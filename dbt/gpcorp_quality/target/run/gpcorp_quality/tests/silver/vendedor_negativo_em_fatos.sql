
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: SalesPersonCode não deve ser -1 nas fatos (sentinela removido na Silver)
-- Se retornar registros, o filtro de negativos falhou

SELECT DocEntry, LineNum, SalesPersonCode
FROM "awsdatacatalog"."gpcorp_silver"."quotations"
WHERE SalesPersonCode < 0
LIMIT 10
  
  
      
    ) dbt_internal_test