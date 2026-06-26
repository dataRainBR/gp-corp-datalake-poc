
    
    



select SalesEmployeeCode
from (select * from "awsdatacatalog"."gpcorp_silver"."sales_persons" where _is_current = true) dbt_subquery
where SalesEmployeeCode is null


