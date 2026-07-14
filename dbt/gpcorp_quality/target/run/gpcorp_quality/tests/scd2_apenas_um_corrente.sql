
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: Cada CardCode deve ter exatamente 1 registro com _is_current = true
-- Se retornar registros, há problema no SCD2

SELECT
    CardCode,
    COUNT(*) as qtd_correntes
FROM "awsdatacatalog"."gpcorp_silver"."business_partners"
WHERE _is_current = true
GROUP BY CardCode
HAVING COUNT(*) > 1
  
  
      
    ) dbt_internal_test