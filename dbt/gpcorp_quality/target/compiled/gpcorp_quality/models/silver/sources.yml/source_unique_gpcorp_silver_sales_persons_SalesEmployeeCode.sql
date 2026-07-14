
    
    

select
    SalesEmployeeCode as unique_field,
    count(*) as n_records

from (select * from "awsdatacatalog"."gpcorp_silver"."sales_persons" where _is_current = true) dbt_subquery
where SalesEmployeeCode is not null
group by SalesEmployeeCode
having count(*) > 1


