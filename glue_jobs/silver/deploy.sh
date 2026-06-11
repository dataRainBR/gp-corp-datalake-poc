#!/bin/bash
# Deploy dos scripts Silver para S3
# Uso: bash deploy.sh

S3_BUCKET="gpcorp-datalake"
S3_PREFIX="glue-scripts/silver"

echo "=== Deploying Silver Glue Jobs ==="
echo "Destino: s3://${S3_BUCKET}/${S3_PREFIX}/"
echo ""

# Upload dos scripts Python
for file in config.py utils.py scd2.py iceberg_writer.py job_dimensions.py job_facts.py quality_checks.py; do
    echo "  → $file"
    aws s3 cp "$file" "s3://${S3_BUCKET}/${S3_PREFIX}/${file}"
done

echo ""
echo "=== Deploy concluído ==="
echo ""
echo "Executar manualmente:"
echo "  aws glue start-job-run --job-name gpcorp-silver-dimensions"
echo "  aws glue start-job-run --job-name gpcorp-silver-facts"
echo "  aws glue start-job-run --job-name gpcorp-silver-quality-checks"
echo ""
echo "Ou iniciar o workflow completo:"
echo "  aws glue start-workflow-run --name gpcorp-silver-pipeline"
