
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ItemCode
from (select * from "awsdatacatalog"."gpcorp_silver"."items" where _is_current = true) dbt_subquery
where ItemCode is null



  
  
      
    ) dbt_internal_test