
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CardCode
from (select * from "awsdatacatalog"."gpcorp_silver"."business_partners" where _is_current = true) dbt_subquery
where CardCode is null



  
  
      
    ) dbt_internal_test