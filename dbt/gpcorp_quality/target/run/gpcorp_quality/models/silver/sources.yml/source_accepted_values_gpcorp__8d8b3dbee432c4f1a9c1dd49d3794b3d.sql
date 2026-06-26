
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DocumentStatus as value_field,
        count(*) as n_records

    from "awsdatacatalog"."gpcorp_silver"."quotations"
    group by DocumentStatus

)

select *
from all_values
where value_field not in (
    'bost_Open','bost_Close'
)



  
  
      
    ) dbt_internal_test