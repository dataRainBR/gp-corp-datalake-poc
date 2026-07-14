
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mes
from "awsdatacatalog"."gpcorp_gold_vendas"."faturamento_mensal"
where mes is null



  
  
      
    ) dbt_internal_test