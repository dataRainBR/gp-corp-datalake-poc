-- Teste: soma de receita na vendas_detalhada deve bater com faturamento_mensal
-- Compara total por ano/mes/vendedor/cliente/item/filial
-- Tolerância de 0.01 para arredondamentos

WITH detalhe AS (
    SELECT ano, mes, cod_vendedor, cod_cliente, cod_item, cod_filial,
           SUM(valor_linha) as receita_detalhe
    FROM "awsdatacatalog"."gpcorp_gold_vendas"."vendas_detalhada"
    GROUP BY ano, mes, cod_vendedor, cod_cliente, cod_item, cod_filial
),
faturamento AS (
    SELECT ano, mes, cod_vendedor, cod_cliente, cod_item, cod_filial,
           receita_total
    FROM "awsdatacatalog"."gpcorp_gold_vendas"."faturamento_mensal"
)
SELECT d.ano, d.mes, d.cod_vendedor, d.cod_cliente,
       d.receita_detalhe, f.receita_total,
       ABS(d.receita_detalhe - f.receita_total) as divergencia
FROM detalhe d
JOIN faturamento f
    ON d.ano = f.ano AND d.mes = f.mes
    AND d.cod_vendedor = f.cod_vendedor
    AND d.cod_cliente = f.cod_cliente
    AND d.cod_item = f.cod_item
    AND d.cod_filial = f.cod_filial
WHERE ABS(d.receita_detalhe - f.receita_total) > 0.01
LIMIT 10