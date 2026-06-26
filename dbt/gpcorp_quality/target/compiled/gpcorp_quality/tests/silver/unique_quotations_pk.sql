-- Teste: PK composta (DocEntry + LineNum) deve ser única em quotations
-- Retorna linhas que violam (duplicatas). dbt considera teste OK se retorno vazio.

SELECT
    DocEntry,
    LineNum,
    COUNT(*) as qtd
FROM "awsdatacatalog"."gpcorp_silver"."quotations"
GROUP BY DocEntry, LineNum
HAVING COUNT(*) > 1