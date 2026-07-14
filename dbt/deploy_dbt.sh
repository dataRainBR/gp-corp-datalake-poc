#!/bin/bash
# Deploy do projeto dbt + criação do Glue Python Shell job
# Uso: bash deploy_dbt.sh

S3_BUCKET="gpcorp-datalake"
ACCOUNT_ID="892748149777"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/GlueServiceRole-gpcorp"

echo "=== Deploy dbt project para S3 ==="

# 1. Sobe o projeto dbt
aws s3 cp gpcorp_quality/dbt_project.yml s3://${S3_BUCKET}/dbt/gpcorp_quality/dbt_project.yml
aws s3 cp gpcorp_quality/profiles.yml s3://${S3_BUCKET}/dbt/gpcorp_quality/profiles.yml
aws s3 cp gpcorp_quality/packages.yml s3://${S3_BUCKET}/dbt/gpcorp_quality/packages.yml
aws s3 cp gpcorp_quality/models/silver/sources.yml s3://${S3_BUCKET}/dbt/gpcorp_quality/models/silver/sources.yml
aws s3 cp gpcorp_quality/tests/unique_quotations_pk.sql s3://${S3_BUCKET}/dbt/gpcorp_quality/tests/unique_quotations_pk.sql
aws s3 cp gpcorp_quality/tests/unique_invoices_pk.sql s3://${S3_BUCKET}/dbt/gpcorp_quality/tests/unique_invoices_pk.sql

# 2. Sobe o script do job
aws s3 cp run_dbt_tests.py s3://${S3_BUCKET}/glue-scripts/dbt/run_dbt_tests.py

echo ""
echo "=== Criando Glue Python Shell job ==="

aws glue create-job \
  --name gpcorp-dbt-tests \
  --role "${ROLE_ARN}" \
  --command '{"Name":"pythonshell","ScriptLocation":"s3://gpcorp-datalake/glue-scripts/dbt/run_dbt_tests.py","PythonVersion":"3.9"}' \
  --default-arguments '{"--additional-python-modules":"dbt-athena-community==1.8.*,elementary-data==0.15.*","--TempDir":"s3://gpcorp-datalake/glue-temp/"}' \
  --max-capacity 1.0 \
  --glue-version "3.0"

echo ""
echo "=== Deploy concluído ==="
echo "Executar: aws glue start-job-run --job-name gpcorp-dbt-tests"
