#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Reprocessamento Gold — Limpar resíduos e rodar todos os jobs
# Executar em Git Bash (PowerShell quebra JSON inline)
# ═══════════════════════════════════════════════════════════════

set -e

echo "═══════════════════════════════════════════════════════════════"
echo "  GOLD — Reprocessamento Completo"
echo "═══════════════════════════════════════════════════════════════"

# ─── 1. Limpar resíduos S3 (tabelas antigas, paths errados) ───
echo ""
echo "[LIMPEZA] Removendo dados Gold existentes no S3..."
aws s3 rm s3://gpcorp-datalake/Gold/ --recursive --quiet
echo "✓ S3 Gold limpo"

# ─── 2. Dropar tabelas existentes no Glue Catalog ───
echo ""
echo "[LIMPEZA] Removendo tabelas do Catalog..."

# Vendas
for tbl in faturamento_mensal ranking_vendedores vendas_por_produto vendas_detalhada; do
    aws glue delete-table --database-name gpcorp_gold_vendas --name $tbl 2>/dev/null && echo "  ✓ vendas.$tbl" || echo "  - vendas.$tbl (não existia)"
done

# Cotações
for tbl in taxa_conversao features_predicao_conversao; do
    aws glue delete-table --database-name gpcorp_gold_cotacoes --name $tbl 2>/dev/null && echo "  ✓ cotacoes.$tbl" || echo "  - cotacoes.$tbl (não existia)"
done

# Cadastros
for tbl in analise_credito; do
    aws glue delete-table --database-name gpcorp_gold_cadastros --name $tbl 2>/dev/null && echo "  ✓ cadastros.$tbl" || echo "  - cadastros.$tbl (não existia)"
done

# Estoque
for tbl in movimentacao_estoque; do
    aws glue delete-table --database-name gpcorp_gold_estoque --name $tbl 2>/dev/null && echo "  ✓ estoque.$tbl" || echo "  - estoque.$tbl (não existia)"
done

echo "✓ Catalog limpo"

# ─── 3. Upload scripts atualizados para S3 ───
echo ""
echo "[UPLOAD] Subindo scripts Gold para S3..."
aws s3 cp glue_jobs/gold/config.py s3://gpcorp-datalake/glue-scripts/gold/config.py
aws s3 cp glue_jobs/gold/job_dashboards.py s3://gpcorp-datalake/glue-scripts/gold/job_dashboards.py
aws s3 cp glue_jobs/gold/job_features_predicao_conversao.py s3://gpcorp-datalake/glue-scripts/gold/job_features_predicao_conversao.py
aws s3 cp glue_jobs/gold/job_credit_features.py s3://gpcorp-datalake/glue-scripts/gold/job_credit_features.py
aws s3 cp glue_jobs/gold/job_estoque.py s3://gpcorp-datalake/glue-scripts/gold/job_estoque.py
echo "✓ Scripts atualizados no S3"

# ─── 4. Rodar job Gold Dashboards (vendas + cotações + ranking) ───
echo ""
echo "[RUN] Iniciando gpcorp-gold-dashboards..."
DASH_RUN=$(aws glue start-job-run \
    --job-name gpcorp-gold-dashboards \
    --arguments '{"--extra-py-files":"s3://gpcorp-datalake/glue-scripts/gold/config.py"}' \
    --query 'JobRunId' --output text)
echo "  JobRunId: $DASH_RUN"

# ─── 5. Rodar job Gold Features Predição Conversão ───
echo ""
echo "[RUN] Iniciando gpcorp-gold-features..."
FEAT_RUN=$(aws glue start-job-run \
    --job-name gpcorp-gold-features \
    --arguments '{"--extra-py-files":"s3://gpcorp-datalake/glue-scripts/gold/config.py"}' \
    --query 'JobRunId' --output text)
echo "  JobRunId: $FEAT_RUN"

# ─── 6. Rodar job Gold Credit Features ───
echo ""
echo "[RUN] Iniciando gpcorp-gold-credit-features..."
CRED_RUN=$(aws glue start-job-run \
    --job-name gpcorp-gold-credit-features \
    --arguments '{"--extra-py-files":"s3://gpcorp-datalake/glue-scripts/gold/config.py"}' \
    --query 'JobRunId' --output text)
echo "  JobRunId: $CRED_RUN"

# ─── 7. Rodar job Gold Estoque ───
echo ""
echo "[RUN] Iniciando gpcorp-gold-estoque..."
EST_RUN=$(aws glue start-job-run \
    --job-name gpcorp-gold-estoque \
    --arguments '{"--extra-py-files":"s3://gpcorp-datalake/glue-scripts/gold/config.py"}' \
    --query 'JobRunId' --output text)
echo "  JobRunId: $EST_RUN"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Jobs Gold iniciados! Monitorar via console ou:"
echo ""
echo "  aws glue get-job-run --job-name gpcorp-gold-dashboards --run-id $DASH_RUN --query 'JobRun.JobRunState'"
echo "  aws glue get-job-run --job-name gpcorp-gold-features --run-id $FEAT_RUN --query 'JobRun.JobRunState'"
echo "  aws glue get-job-run --job-name gpcorp-gold-credit-features --run-id $CRED_RUN --query 'JobRun.JobRunState'"
echo "  aws glue get-job-run --job-name gpcorp-gold-estoque --run-id $EST_RUN --query 'JobRun.JobRunState'"
echo "═══════════════════════════════════════════════════════════════"
