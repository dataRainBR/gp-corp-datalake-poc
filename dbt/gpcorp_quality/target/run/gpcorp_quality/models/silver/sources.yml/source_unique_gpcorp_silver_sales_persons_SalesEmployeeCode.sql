
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    SalesEmployeeCode as unique_field,
    count(*) as n_records

from (select * from "awsdatacatalog"."gpcorp_silver"."sales_persons" where _is_current = true) dbt_subquery
where SalesEmployeeCode is not null
group by SalesEmployeeCode
having count(*) > 1



  
  
      
    ) dbt_internal_test