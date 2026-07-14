-- Teste: DocTotal deve ser consistente com soma dos LineTotals em invoices
-- Tolerância de 1% para arredondamentos fiscais brasileiros (ICMS-ST, IPI)
WITH doc_totals AS (
    SELECT
        DocEntry,
        DocTotal as total_header,
        SUM(LineTotal) as soma_linhas
    FROM "awsdatacatalog"."gpcorp_silver"."invoices"
    GROUP BY DocEntry, DocTotal
)
SELECT
    DocEntry,
    total_header,
    soma_linhas,
    ABS(total_header - soma_linhas) as divergencia
FROM doc_totals
WHERE total_header > 0
  AND ABS(total_header - soma_linhas) / total_header > 0.01