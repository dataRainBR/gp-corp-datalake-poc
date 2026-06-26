
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    CardCode as unique_field,
    count(*) as n_records

from (select * from "awsdatacatalog"."gpcorp_silver"."business_partners" where _is_current = true) dbt_subquery
where CardCode is not null
group by CardCode
having count(*) > 1



  
  
      
    ) dbt_internal_test