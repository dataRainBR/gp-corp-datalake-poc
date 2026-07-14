
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ranking
from "awsdatacatalog"."gpcorp_gold_vendas"."ranking_vendedores"
where ranking is null



  
  
      
    ) dbt_internal_test