
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select receita_total
from "awsdatacatalog"."gpcorp_gold_vendas"."faturamento_mensal"
where receita_total is null



  
  
      
    ) dbt_internal_test