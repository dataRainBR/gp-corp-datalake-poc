
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select Number
from "awsdatacatalog"."gpcorp_silver"."item_groups"
where Number is null



  
  
      
    ) dbt_internal_test