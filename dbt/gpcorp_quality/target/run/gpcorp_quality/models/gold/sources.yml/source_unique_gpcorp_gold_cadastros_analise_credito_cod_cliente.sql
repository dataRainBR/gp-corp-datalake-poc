
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    cod_cliente as unique_field,
    count(*) as n_records

from "awsdatacatalog"."gpcorp_gold_cadastros"."analise_credito"
where cod_cliente is not null
group by cod_cliente
having count(*) > 1



  
  
      
    ) dbt_internal_test