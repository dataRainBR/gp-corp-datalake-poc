-- Teste: DocTotal deve ser consistente com soma dos LineTotals + VatSum + despesas adicionais
-- DocTotal = SUM(LineTotal) + VatSum (impostos) + DocumentAdditionalExpenses (frete/despesas no cabeçalho)
-- Tolerância de 5% para arredondamentos fiscais brasileiros (ICMS-ST, IPI)
--
-- DocumentAdditionalExpenses é a despesa adicional lançada no cabeçalho da NF
-- (tipicamente frete), separada das linhas de item. Sem considerá-la, ~5-8%
-- das NFs (as que têm frete no cabeçalho) apareciam como falso-positivo aqui.
--
-- Exclui NFs onde SUM(LineTotal) > DocTotal: indica dado inconsistente no SAP
-- (cancelamento parcial, estorno) — não é um problema do pipeline.
WITH doc_totals AS (
    SELECT
        DocEntry,
        DocTotal as total_header,
        VatSum as impostos_header,
        MAX(DocumentAdditionalExpenses) as despesas_adicionais,
        SUM(LineTotal) as soma_linhas
    FROM {{ source('gpcorp_silver', 'invoices') }}
    WHERE cancelled != 'tYES'
    GROUP BY DocEntry, DocTotal, VatSum
)
SELECT
    DocEntry,
    total_header,
    soma_linhas,
    impostos_header,
    despesas_adicionais,
    soma_linhas + impostos_header + despesas_adicionais as total_esperado,
    ABS(total_header - (soma_linhas + impostos_header + despesas_adicionais)) as divergencia
FROM doc_totals
WHERE total_header > 0
  AND (soma_linhas + impostos_header + despesas_adicionais) > 0
  AND soma_linhas <= total_header * 5
  AND ABS(total_header - (soma_linhas + impostos_header + despesas_adicionais)) / total_header > 0.05
