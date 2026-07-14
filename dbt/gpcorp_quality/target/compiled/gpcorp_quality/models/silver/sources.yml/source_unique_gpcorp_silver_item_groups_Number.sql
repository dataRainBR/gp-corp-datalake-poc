
    
    

select
    Number as unique_field,
    count(*) as n_records

from "awsdatacatalog"."gpcorp_silver"."item_groups"
where Number is not null
group by Number
having count(*) > 1


