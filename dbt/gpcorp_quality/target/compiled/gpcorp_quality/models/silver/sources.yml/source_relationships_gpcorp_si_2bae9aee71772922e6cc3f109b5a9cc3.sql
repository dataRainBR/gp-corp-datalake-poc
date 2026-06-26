
    
    

with child as (
    select CardCode as from_field
    from (select * from "awsdatacatalog"."gpcorp_silver"."quotations" where CardCode IS NOT NULL) dbt_subquery
    where CardCode is not null
),

parent as (
    select CardCode as to_field
    from "awsdatacatalog"."gpcorp_silver"."business_partners"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


