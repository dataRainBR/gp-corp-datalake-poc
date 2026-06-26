-- Teste: decisao_final deve ser um valor esperado
SELECT cod_cliente, nome_cliente, decisao_final
FROM {{ source('gpcorp_gold_cadastros', 'analise_credito') }}
WHERE decisao_final IS NOT NULL
  AND decisao_final NOT IN ('aprovado_prazo', 'apenas_vista', 'limite_zerado')
LIMIT 10
