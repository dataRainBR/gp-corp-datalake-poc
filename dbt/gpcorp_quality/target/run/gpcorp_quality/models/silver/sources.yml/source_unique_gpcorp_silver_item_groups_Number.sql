
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    Number as unique_field,
    count(*) as n_records

from "awsdatacatalog"."gpcorp_silver"."item_groups"
where Number is not null
group by Number
having count(*) > 1



  
  
      
    ) dbt_internal_test