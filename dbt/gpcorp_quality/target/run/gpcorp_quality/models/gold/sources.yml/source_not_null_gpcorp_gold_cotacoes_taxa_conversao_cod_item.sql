
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cod_item
from "awsdatacatalog"."gpcorp_gold_cotacoes"."taxa_conversao"
where cod_item is null



  
  
      
    ) dbt_internal_test