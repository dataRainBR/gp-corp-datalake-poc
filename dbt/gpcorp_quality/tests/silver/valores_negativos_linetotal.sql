-- Teste: LineTotal negativo pode indicar devolução — aceitável
-- Mas se > 5% das linhas forem negativas, pode ser problema de dados
-- Retorna se percentual ultrapassa 5%

WITH stats AS (
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN LineTotal < 0 THEN 1 ELSE 0 END) as negativos
    FROM {{ source('gpcorp_silver', 'quotations') }}
)
SELECT
    total,
    negativos,
    ROUND(negativos * 100.0 / total, 2) as pct_negativos
FROM stats
WHERE negativos * 100.0 / total > 5.0
