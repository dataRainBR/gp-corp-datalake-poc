
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select LineNum
from "awsdatacatalog"."gpcorp_silver"."orders"
where LineNum is null



  
  
      
    ) dbt_internal_test