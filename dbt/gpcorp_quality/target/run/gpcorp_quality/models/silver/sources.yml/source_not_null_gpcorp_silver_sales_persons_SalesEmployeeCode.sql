
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select SalesEmployeeCode
from (select * from "awsdatacatalog"."gpcorp_silver"."sales_persons" where _is_current = true) dbt_subquery
where SalesEmployeeCode is null



  
  
      
    ) dbt_internal_test