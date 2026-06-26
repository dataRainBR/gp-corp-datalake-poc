
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DocEntry
from "awsdatacatalog"."gpcorp_silver"."orders"
where DocEntry is null



  
  
      
    ) dbt_internal_test