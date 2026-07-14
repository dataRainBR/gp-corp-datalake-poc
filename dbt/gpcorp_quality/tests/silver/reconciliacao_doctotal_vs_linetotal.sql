-- Teste: DocTotal de cotações deve ser próximo à soma dos LineTotals + VatSum
-- Tolerância de 5% (cotações podem ter impostos estimados)

WITH doc_totals AS (
    SELECT
        DocEntry,
        DocTotal as total_header,
        VatSum as impostos_header,
        SUM(LineTotal) as soma_linhas
    FROM {{ source('gpcorp_silver', 'quotations') }}
    GROUP BY DocEntry, DocTotal, VatSum
)
SELECT
    DocEntry,
    total_header,
    soma_linhas,
    impostos_header,
    ABS(total_header - (soma_linhas + impostos_header)) as divergencia
FROM doc_totals
WHERE total_header > 0
  AND (soma_linhas + impostos_header) > 0
  AND ABS(total_header - (soma_linhas + impostos_header)) / total_header > 0.05
