
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cod_item
from "awsdatacatalog"."gpcorp_gold_vendas"."vendas_por_produto"
where cod_item is null



  
  
      
    ) dbt_internal_test