
    
    



select ItemCode
from (select * from "awsdatacatalog"."gpcorp_silver"."items" where _is_current = true) dbt_subquery
where ItemCode is null


