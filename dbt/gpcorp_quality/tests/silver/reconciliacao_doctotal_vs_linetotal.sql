-- Teste: DocTotal deve ser igual (ou próximo) à soma dos LineTotals por documento
-- Tolerância de 0.1% para arredondamentos fiscais
-- Retorna documentos com divergência > 0.1%

WITH doc_totals AS (
    SELECT
        DocEntry,
        DocTotal as total_header,
        SUM(LineTotal) as soma_linhas
    FROM {{ source('gpcorp_silver', 'quotations') }}
    GROUP BY DocEntry, DocTotal
)
SELECT
    DocEntry,
    total_header,
    soma_linhas,
    ABS(total_header - soma_linhas) as divergencia
FROM doc_totals
WHERE total_header > 0
  AND ABS(total_header - soma_linhas) / total_header > 0.001
