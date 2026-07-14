
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: receita por produto deve ser > 0 (agregação de vendas)
SELECT ano, mes, cod_item, nome_item, receita
FROM "awsdatacatalog"."gpcorp_gold_vendas"."vendas_por_produto"
WHERE receita <= 0
LIMIT 10
  
  
      
    ) dbt_internal_test