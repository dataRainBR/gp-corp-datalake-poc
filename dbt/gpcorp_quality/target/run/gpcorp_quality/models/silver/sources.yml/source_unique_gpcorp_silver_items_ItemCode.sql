
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    ItemCode as unique_field,
    count(*) as n_records

from (select * from "awsdatacatalog"."gpcorp_silver"."items" where _is_current = true) dbt_subquery
where ItemCode is not null
group by ItemCode
having count(*) > 1



  
  
      
    ) dbt_internal_test