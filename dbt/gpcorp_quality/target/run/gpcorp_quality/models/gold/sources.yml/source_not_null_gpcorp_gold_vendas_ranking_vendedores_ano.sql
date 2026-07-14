
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ano
from "awsdatacatalog"."gpcorp_gold_vendas"."ranking_vendedores"
where ano is null



  
  
      
    ) dbt_internal_test