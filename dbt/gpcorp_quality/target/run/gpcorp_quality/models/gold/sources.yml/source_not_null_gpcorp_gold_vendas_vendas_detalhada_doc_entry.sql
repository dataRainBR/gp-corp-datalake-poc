
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select doc_entry
from "awsdatacatalog"."gpcorp_gold_vendas"."vendas_detalhada"
where doc_entry is null



  
  
      
    ) dbt_internal_test