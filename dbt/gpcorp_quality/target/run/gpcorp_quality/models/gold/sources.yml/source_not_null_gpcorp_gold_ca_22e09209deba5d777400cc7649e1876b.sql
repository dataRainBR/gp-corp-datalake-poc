
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cod_cliente
from "awsdatacatalog"."gpcorp_gold_cadastros"."analise_credito"
where cod_cliente is null



  
  
      
    ) dbt_internal_test