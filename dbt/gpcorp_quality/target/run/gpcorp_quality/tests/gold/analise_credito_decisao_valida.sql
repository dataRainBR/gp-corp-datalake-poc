
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: decisao_final deve ser um valor esperado
SELECT cod_cliente, nome_cliente, decisao_final
FROM "awsdatacatalog"."gpcorp_gold_cadastros"."analise_credito"
WHERE decisao_final IS NOT NULL
  AND decisao_final NOT IN ('aprovado_prazo', 'apenas_vista', 'limite_zerado')
LIMIT 10
  
  
      
    ) dbt_internal_test