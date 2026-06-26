
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cod_vendedor
from "awsdatacatalog"."gpcorp_gold_cotacoes"."features_predicao_conversao"
where cod_vendedor is null



  
  
      
    ) dbt_internal_test