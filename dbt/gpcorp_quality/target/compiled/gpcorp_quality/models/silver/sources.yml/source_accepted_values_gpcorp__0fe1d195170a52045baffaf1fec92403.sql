
    
    

with all_values as (

    select
        CardType as value_field,
        count(*) as n_records

    from (select * from "awsdatacatalog"."gpcorp_silver"."business_partners" where _is_current = true) dbt_subquery
    group by CardType

)

select *
from all_values
where value_field not in (
    'cCustomer','cSupplier','cLead'
)


