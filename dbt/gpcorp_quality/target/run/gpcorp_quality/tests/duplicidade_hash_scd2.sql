
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: _row_hash não deve repetir para registros correntes (mesmo CardCode)
-- Se repetir, indica que dois registros idênticos foram marcados como correntes

SELECT CardCode, _row_hash, COUNT(*) as qtd
FROM "awsdatacatalog"."gpcorp_silver"."business_partners"
WHERE _is_current = true
GROUP BY CardCode, _row_hash
HAVING COUNT(*) > 1
  
  
      
    ) dbt_internal_test