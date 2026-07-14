
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select num_linha
from "awsdatacatalog"."gpcorp_gold_vendas"."vendas_detalhada"
where num_linha is null



  
  
      
    ) dbt_internal_test