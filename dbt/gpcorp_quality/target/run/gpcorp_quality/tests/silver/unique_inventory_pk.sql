
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Teste: PK composta (DocEntry + LineNum) única em inventory_gen_entries
SELECT DocEntry, LineNum, COUNT(*) as qtd
FROM "awsdatacatalog"."gpcorp_silver"."inventory_gen_entries"
GROUP BY DocEntry, LineNum
HAVING COUNT(*) > 1
  
  
      
    ) dbt_internal_test