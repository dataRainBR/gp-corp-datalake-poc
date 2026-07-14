#!/bin/bash
# Cria databases Gold organizados por domínio de negócio

echo "=== Criando databases Gold por domínio ==="

aws glue create-database --database-input '{
  "Name": "gpcorp_gold_vendas",
  "Description": "Gold — Agregações comerciais: faturamento, ranking, mix de produtos",
  "LocationUri": "s3://gpcorp-datalake/Gold/vendas/"
}' 2>/dev/null && echo "  ✓ gpcorp_gold_vendas" || echo "  ⚠ gpcorp_gold_vendas já existe"

aws glue create-database --database-input '{
  "Name": "gpcorp_gold_cotacoes",
  "Description": "Gold — Análise de pipeline e conversão (taxa_conversao)",
  "LocationUri": "s3://gpcorp-datalake/Gold/cotacoes/"
}' 2>/dev/null && echo "  ✓ gpcorp_gold_cotacoes" || echo "  ⚠ gpcorp_gold_cotacoes já existe"

aws glue create-database --database-input '{
  "Name": "gpcorp_gold_cadastros",
  "Description": "Gold — Features de clientes e crédito (analise_credito)",
  "LocationUri": "s3://gpcorp-datalake/Gold/cadastros/"
}' 2>/dev/null && echo "  ✓ gpcorp_gold_cadastros" || echo "  ⚠ gpcorp_gold_cadastros já existe"

aws glue create-database --database-input '{
  "Name": "gpcorp_gold_estoque",
  "Description": "Gold — Análise de inventário e movimentação (fase 2)",
  "LocationUri": "s3://gpcorp-datalake/Gold/estoque/"
}' 2>/dev/null && echo "  ✓ gpcorp_gold_estoque" || echo "  ⚠ gpcorp_gold_estoque já existe"

echo ""
echo "=== Databases Gold criados ==="
echo ""
echo "Tabelas por domínio:"
echo "  gpcorp_gold_vendas.faturamento_mensal"
echo "  gpcorp_gold_vendas.ranking_vendedores"
echo "  gpcorp_gold_vendas.vendas_por_produto"
echo "  gpcorp_gold_cotacoes.taxa_conversao"
echo "  gpcorp_gold_cadastros.analise_credito"
echo "  gpcorp_gold_estoque.movimentacao_estoque"
