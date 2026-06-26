#!/bin/bash
# Cria o Glue Job gpcorp-gold-credit-features
# Executar em Git Bash

set -e

echo "[CREATE] Criando job gpcorp-gold-credit-features..."

aws glue create-job \
    --name "gpcorp-gold-credit-features" \
    --role "arn:aws:iam::892748149777:role/GlueServiceRole-gpcorp" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://gpcorp-datalake/glue-scripts/gold/job_credit_features.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--extra-py-files": "s3://gpcorp-datalake/glue-scripts/gold/config.py",
        "--datalake-formats": "iceberg",
        "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--enable-auto-scaling": "true",
        "--enable-continuous-cloudwatch-log": "false",
        "--enable-metrics": "true",
        "--TempDir": "s3://gpcorp-datalake/glue-temp/"
    }' \
    --glue-version "4.0" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --timeout 30 \
    --region us-east-1

echo "✓ Job gpcorp-gold-credit-features criado"
