#!/bin/bash
# Deploy dos scripts Gold para S3
# Uso: bash deploy.sh

S3_BUCKET="gpcorp-datalake"
S3_PREFIX="glue-scripts/gold"

echo "=== Deploying Gold Glue Jobs ==="
echo "Destino: s3://${S3_BUCKET}/${S3_PREFIX}/"
echo ""

for file in config.py job_dashboards.py job_features.py; do
    echo "  → $file"
    aws s3 cp "$file" "s3://${S3_BUCKET}/${S3_PREFIX}/${file}"
done

echo ""
echo "=== Deploy concluído ==="
echo ""
echo "Executar manualmente:"
echo "  aws glue start-job-run --job-name gpcorp-gold-dashboards"
echo "  aws glue start-job-run --job-name gpcorp-gold-features"
