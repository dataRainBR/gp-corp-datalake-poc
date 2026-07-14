
    
    



select CardName
from (select * from "awsdatacatalog"."gpcorp_silver"."business_partners" where _is_current = true) dbt_subquery
where CardName is null


