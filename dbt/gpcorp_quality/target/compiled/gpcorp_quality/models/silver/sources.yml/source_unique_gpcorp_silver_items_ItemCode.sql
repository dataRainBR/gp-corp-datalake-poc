
    
    

select
    ItemCode as unique_field,
    count(*) as n_records

from (select * from "awsdatacatalog"."gpcorp_silver"."items" where _is_current = true) dbt_subquery
where ItemCode is not null
group by ItemCode
having count(*) > 1


