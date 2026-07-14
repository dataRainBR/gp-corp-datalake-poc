
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: pct_margem_bruta deve estar em range razoável (-100% a 100%)
-- Valores fora indicam erro de cálculo
SELECT ano, mes, cod_vendedor, nome_vendedor, pct_margem_bruta
FROM "awsdatacatalog"."gpcorp_gold_vendas"."ranking_vendedores"
WHERE pct_margem_bruta < -100 OR pct_margem_bruta > 100
LIMIT 10
  
  
      
    ) dbt_internal_test