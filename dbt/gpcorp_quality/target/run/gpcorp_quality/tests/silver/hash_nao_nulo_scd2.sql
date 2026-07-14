
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: _row_hash nunca deve ser nulo nas dimensões SCD2
-- Se nulo, a detecção de mudança incremental falha
-- Retorna linhas apenas se houver nulos (0 linhas = teste passa)

WITH contagem AS (
    SELECT 'business_partners' as tabela, COUNT(*) as nulos
    FROM "awsdatacatalog"."gpcorp_silver"."business_partners"
    WHERE _row_hash IS NULL

    UNION ALL

    SELECT 'items' as tabela, COUNT(*) as nulos
    FROM "awsdatacatalog"."gpcorp_silver"."items"
    WHERE _row_hash IS NULL

    UNION ALL

    SELECT 'sales_persons' as tabela, COUNT(*) as nulos
    FROM "awsdatacatalog"."gpcorp_silver"."sales_persons"
    WHERE _row_hash IS NULL
)
SELECT tabela, nulos
FROM contagem
WHERE nulos > 0
  
  
      
    ) dbt_internal_test