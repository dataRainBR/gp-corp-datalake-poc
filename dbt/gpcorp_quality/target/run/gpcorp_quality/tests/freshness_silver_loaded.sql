
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: dados Silver devem ter sido carregados nas últimas 48h (SLA)
-- Se retornar resultado, a carga está atrasada

SELECT
    MAX(_silver_loaded_at) as ultima_carga,
    CURRENT_TIMESTAMP as agora,
    DATE_DIFF('hour', MAX(_silver_loaded_at), CURRENT_TIMESTAMP) as horas_desde_carga
FROM "awsdatacatalog"."gpcorp_silver"."business_partners"
HAVING DATE_DIFF('hour', MAX(_silver_loaded_at), CURRENT_TIMESTAMP) > 48
  
  
      
    ) dbt_internal_test