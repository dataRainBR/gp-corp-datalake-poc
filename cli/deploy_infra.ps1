# =============================================================================
# Deploy Infraestrutura via CLI — SQS, Alertas, IAM, Lake Formation
# Executar com AWS CLI configurado na conta 892748149777
# =============================================================================

$ErrorActionPreference = "Continue"
$ACCOUNT = "892748149777"
$REGION = "us-east-1"
$SNS_TOPIC = "arn:aws:sns:us-east-1:892748149777:gpcorp-glue-pipeline-alerts"

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  GP Corp Datalake — Deploy Infraestrutura" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan

# ═══════════════════════════════════════════════════════════════
# 1. SQS — Retry Queue + DLQ
# ═══════════════════════════════════════════════════════════════

Write-Host "`n[1/5] SQS Retry Queue + DLQ" -ForegroundColor Yellow

# DLQ primeiro (retry precisa do ARN)
Write-Host "  Criando DLQ..."
$dlqUrl = aws sqs create-queue --queue-name gpcorp-extraction-dlq --attributes "MessageRetentionPeriod=1209600,VisibilityTimeout=60" --query "QueueUrl" --output text 2>$null
if ($dlqUrl) {
    Write-Host "  OK: $dlqUrl" -ForegroundColor Green
    $dlqArn = aws sqs get-queue-attributes --queue-url $dlqUrl --attribute-names QueueArn --query "Attributes.QueueArn" --output text 2>$null
} else {
    $dlqUrl = aws sqs get-queue-url --queue-name gpcorp-extraction-dlq --query "QueueUrl" --output text 2>$null
    $dlqArn = aws sqs get-queue-attributes --queue-url $dlqUrl --attribute-names QueueArn --query "Attributes.QueueArn" --output text 2>$null
    Write-Host "  Ja existe: $dlqUrl" -ForegroundColor Gray
}

# Retry Queue com redrive para DLQ
Write-Host "  Criando Retry Queue..."
$redrivePolicy = "{""deadLetterTargetArn"":""$dlqArn"",""maxReceiveCount"":""3""}"
$retryUrl = aws sqs create-queue --queue-name gpcorp-extraction-retry --attributes "DelaySeconds=60,MessageRetentionPeriod=86400,VisibilityTimeout=120,ReceiveMessageWaitTimeSeconds=20,RedrivePolicy=$redrivePolicy" --query "QueueUrl" --output text 2>$null
if ($retryUrl) {
    Write-Host "  OK: $retryUrl" -ForegroundColor Green
} else {
    $retryUrl = aws sqs get-queue-url --queue-name gpcorp-extraction-retry --query "QueueUrl" --output text 2>$null
    Write-Host "  Ja existe: $retryUrl" -ForegroundColor Gray
}

# ═══════════════════════════════════════════════════════════════
# 2. CloudWatch Alarms — novos
# ═══════════════════════════════════════════════════════════════

Write-Host "`n[2/5] CloudWatch Alarms" -ForegroundColor Yellow

# Alarme: DLQ não vazia
Write-Host "  Criando alarme DLQ..."
aws cloudwatch put-metric-alarm `
    --alarm-name "gpcorp-extraction-dlq-not-empty" `
    --alarm-description "Existem extracoes que falharam permanentemente na DLQ" `
    --namespace "AWS/SQS" `
    --metric-name "ApproximateNumberOfMessagesVisible" `
    --dimensions "Name=QueueName,Value=gpcorp-extraction-dlq" `
    --statistic Maximum `
    --period 300 `
    --evaluation-periods 1 `
    --threshold 0 `
    --comparison-operator GreaterThanThreshold `
    --treat-missing-data notBreaching `
    --alarm-actions $SNS_TOPIC 2>$null
Write-Host "  OK: gpcorp-extraction-dlq-not-empty" -ForegroundColor Green

# Alarme: Step Functions falhou
Write-Host "  Criando alarme Step Functions..."
aws cloudwatch put-metric-alarm `
    --alarm-name "gpcorp-pipeline-sfn-failed" `
    --alarm-description "Step Functions pipeline falhou" `
    --namespace "AWS/States" `
    --metric-name "ExecutionsFailed" `
    --dimensions "Name=StateMachineArn,Value=arn:aws:states:us-east-1:892748149777:stateMachine:GPCorp_data_lake_pipeline" `
    --statistic Sum `
    --period 86400 `
    --evaluation-periods 1 `
    --threshold 0 `
    --comparison-operator GreaterThanThreshold `
    --treat-missing-data notBreaching `
    --alarm-actions $SNS_TOPIC 2>$null
Write-Host "  OK: gpcorp-pipeline-sfn-failed" -ForegroundColor Green

# Alarme: Step Functions não executou (missing = breaching)
Write-Host "  Criando alarme pipeline nao executou..."
aws cloudwatch put-metric-alarm `
    --alarm-name "gpcorp-pipeline-not-executed" `
    --alarm-description "Pipeline nao executou nas ultimas 26h" `
    --namespace "AWS/States" `
    --metric-name "ExecutionsStarted" `
    --dimensions "Name=StateMachineArn,Value=arn:aws:states:us-east-1:892748149777:stateMachine:GPCorp_data_lake_pipeline" `
    --statistic Sum `
    --period 93600 `
    --evaluation-periods 1 `
    --threshold 1 `
    --comparison-operator LessThanThreshold `
    --treat-missing-data breaching `
    --alarm-actions $SNS_TOPIC 2>$null
Write-Host "  OK: gpcorp-pipeline-not-executed" -ForegroundColor Green

# Alarme: Gold Estoque failure
Write-Host "  Criando alarme gold-estoque..."
aws cloudwatch put-metric-alarm `
    --alarm-name "gpcorp-gold-estoque-failure" `
    --alarm-description "Job gpcorp-gold-estoque falhou" `
    --namespace "Glue" `
    --metric-name "glue.driver.aggregate.numFailedTasks" `
    --dimensions "Name=JobName,Value=gpcorp-gold-estoque" `
    --statistic Sum `
    --period 300 `
    --evaluation-periods 1 `
    --threshold 0 `
    --comparison-operator GreaterThanThreshold `
    --treat-missing-data notBreaching `
    --alarm-actions $SNS_TOPIC 2>$null
Write-Host "  OK: gpcorp-gold-estoque-failure" -ForegroundColor Green

# Alarme: Gold Cadastros failure
Write-Host "  Criando alarme gold-cadastros..."
aws cloudwatch put-metric-alarm `
    --alarm-name "gpcorp-gold-cadastros-failure" `
    --alarm-description "Job gpcorp-gold-cadastros falhou" `
    --namespace "Glue" `
    --metric-name "glue.driver.aggregate.numFailedTasks" `
    --dimensions "Name=JobName,Value=gpcorp-gold-cadastros" `
    --statistic Sum `
    --period 300 `
    --evaluation-periods 1 `
    --threshold 0 `
    --comparison-operator GreaterThanThreshold `
    --treat-missing-data notBreaching `
    --alarm-actions $SNS_TOPIC 2>$null
Write-Host "  OK: gpcorp-gold-cadastros-failure" -ForegroundColor Green

# Alarme: Compaction failure
Write-Host "  Criando alarme compaction..."
aws cloudwatch put-metric-alarm `
    --alarm-name "gpcorp-silver-compaction-failure" `
    --alarm-description "Job gpcorp-silver-compaction falhou" `
    --namespace "Glue" `
    --metric-name "glue.driver.aggregate.numFailedTasks" `
    --dimensions "Name=JobName,Value=gpcorp-silver-compaction" `
    --statistic Sum `
    --period 300 `
    --evaluation-periods 1 `
    --threshold 0 `
    --comparison-operator GreaterThanThreshold `
    --treat-missing-data notBreaching `
    --alarm-actions $SNS_TOPIC 2>$null
Write-Host "  OK: gpcorp-silver-compaction-failure" -ForegroundColor Green

Write-Host "`n[2/5] Alarms concluido." -ForegroundColor Green


# ═══════════════════════════════════════════════════════════════
# 3. IAM Roles — separação de responsabilidades
# ═══════════════════════════════════════════════════════════════

Write-Host "`n[3/5] IAM Roles" -ForegroundColor Yellow

# Nota: a role GlueServiceRole-gpcorp ja existe e tem permissões amplas (POC)
# Aqui vamos criar a role Analyst separada para QuickSight

$analystTrustPolicy = @"
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "quicksight.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
"@

Write-Host "  Criando role AnalystRole-gpcorp..."
$analystTrust = $analystTrustPolicy | ConvertTo-Json -Compress
try {
    aws iam create-role --role-name "AnalystRole-gpcorp" --assume-role-policy-document $analystTrustPolicy --tags Key=Project,Value=gpcorp-datalake --output text 2>$null
    Write-Host "  OK: AnalystRole-gpcorp criada" -ForegroundColor Green
} catch {
    Write-Host "  Ja existe" -ForegroundColor Gray
}

# Policy Analyst: read Gold + Silver, Athena, Glue Catalog
$analystPolicy = @"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AthenaQuery",
      "Effect": "Allow",
      "Action": ["athena:StartQueryExecution","athena:GetQueryExecution","athena:GetQueryResults","athena:StopQueryExecution"],
      "Resource": "arn:aws:athena:us-east-1:892748149777:workgroup/primary"
    },
    {
      "Sid": "S3Read",
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:ListBucket","s3:PutObject"],
      "Resource": [
        "arn:aws:s3:::gpcorp-datalake/Gold/*",
        "arn:aws:s3:::gpcorp-datalake/Silver/*",
        "arn:aws:s3:::gpcorp-datalake/athena-results/*",
        "arn:aws:s3:::gpcorp-datalake"
      ]
    },
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": ["glue:GetDatabase","glue:GetDatabases","glue:GetTable","glue:GetTables","glue:GetPartitions"],
      "Resource": [
        "arn:aws:glue:us-east-1:892748149777:catalog",
        "arn:aws:glue:us-east-1:892748149777:database/gpcorp_gold_*",
        "arn:aws:glue:us-east-1:892748149777:database/gpcorp_silver",
        "arn:aws:glue:us-east-1:892748149777:table/gpcorp_gold_*/*",
        "arn:aws:glue:us-east-1:892748149777:table/gpcorp_silver/*"
      ]
    },
    {
      "Sid": "LakeFormation",
      "Effect": "Allow",
      "Action": ["lakeformation:GetDataAccess"],
      "Resource": "*"
    }
  ]
}
"@

Write-Host "  Adicionando policy ao AnalystRole..."
aws iam put-role-policy --role-name "AnalystRole-gpcorp" --policy-name "gpcorp-analyst-access" --policy-document $analystPolicy 2>$null
Write-Host "  OK: Policy aplicada" -ForegroundColor Green

# ═══════════════════════════════════════════════════════════════
# 4. Lake Formation — RBAC
# ═══════════════════════════════════════════════════════════════

Write-Host "`n[4/5] Lake Formation" -ForegroundColor Yellow

$glueRole = "arn:aws:iam::892748149777:role/GlueServiceRole-gpcorp"
$analystRole = "arn:aws:iam::892748149777:role/AnalystRole-gpcorp"

# Admin: Glue role tem acesso total
Write-Host "  Configurando admin..."
$lfAdminSettings = '{"DataLakeAdmins":[{"DataLakePrincipalIdentifier":"' + $glueRole + '"}]}'
aws lakeformation put-data-lake-settings --data-lake-settings $lfAdminSettings 2>$null
Write-Host "  OK: GlueServiceRole como admin" -ForegroundColor Green

# Registrar S3 locations
Write-Host "  Registrando locations S3..."
foreach ($path in @("Bronze", "Silver", "Gold")) {
    aws lakeformation register-resource --resource-arn "arn:aws:s3:::gpcorp-datalake/$path" --use-service-linked-role 2>$null
}
Write-Host "  OK: Bronze, Silver, Gold registrados" -ForegroundColor Green

# Permissões Analyst: SELECT em Gold e Silver (PII mascarado)
Write-Host "  Configurando permissoes Analyst..."

$goldDatabases = @("gpcorp_gold_vendas", "gpcorp_gold_cotacoes", "gpcorp_gold_cadastros", "gpcorp_gold_estoque")
$allDatabases = $goldDatabases + @("gpcorp_silver")

# Grants Analyst: DESCRIBE database + SELECT tables
foreach ($db in $allDatabases) {
    $resourceDb = '{"Database":{"Name":"' + $db + '"}}'
    $resourceTbl = '{"Table":{"DatabaseName":"' + $db + '","TableWildcard":{}}}'
    aws lakeformation grant-permissions --principal "DataLakePrincipalIdentifier=$analystRole" --resource $resourceDb --permissions "DESCRIBE" 2>$null
    aws lakeformation grant-permissions --principal "DataLakePrincipalIdentifier=$analystRole" --resource $resourceTbl --permissions "SELECT" "DESCRIBE" 2>$null
}
Write-Host "  OK: Analyst — SELECT Gold + Silver" -ForegroundColor Green

# Permissões Engineer (Glue ETL role): ALL em Silver + Gold, SELECT Bronze
Write-Host "  Configurando permissoes Engineer (GlueServiceRole)..."

# Bronze: somente leitura
$resBronzeDb = '{"Database":{"Name":"gpcorp_bronze"}}'
$resBronzeTbl = '{"Table":{"DatabaseName":"gpcorp_bronze","TableWildcard":{}}}'
aws lakeformation grant-permissions --principal "DataLakePrincipalIdentifier=$glueRole" --resource $resBronzeDb --permissions "DESCRIBE" 2>$null
aws lakeformation grant-permissions --principal "DataLakePrincipalIdentifier=$glueRole" --resource $resBronzeTbl --permissions "SELECT" "DESCRIBE" 2>$null

# Silver + Gold: ALL
foreach ($db in $allDatabases) {
    $resourceDb = '{"Database":{"Name":"' + $db + '"}}'
    $resourceTbl = '{"Table":{"DatabaseName":"' + $db + '","TableWildcard":{}}}'
    aws lakeformation grant-permissions --principal "DataLakePrincipalIdentifier=$glueRole" --resource $resourceDb --permissions "ALL" 2>$null
    aws lakeformation grant-permissions --principal "DataLakePrincipalIdentifier=$glueRole" --resource $resourceTbl --permissions "ALL" 2>$null
}
Write-Host "  OK: Engineer — ALL Silver+Gold, SELECT Bronze" -ForegroundColor Green

# Habilitar Lake Formation: admin + remove default IAM_ALLOWED_PRINCIPALS
Write-Host "  Configurando admin + audit..."
$lfSettings = '{"DataLakeAdmins":[{"DataLakePrincipalIdentifier":"' + $glueRole + '"}],"CreateDatabaseDefaultPermissions":[],"CreateTableDefaultPermissions":[]}'
aws lakeformation put-data-lake-settings --data-lake-settings $lfSettings 2>$null
Write-Host "  OK: Admin configurado, default permissions removidas" -ForegroundColor Green

# ═══════════════════════════════════════════════════════════════
# 5. Lambda Retry (deploy zip)
# ═══════════════════════════════════════════════════════════════

Write-Host "`n[5/5] Lambda Retry" -ForegroundColor Yellow

# Verifica se Lambda ja existe
$lambdaExists = aws lambda get-function --function-name gpcorp-extraction-retry --query "Configuration.FunctionName" --output text 2>$null
if ($lambdaExists) {
    Write-Host "  Lambda ja existe: $lambdaExists" -ForegroundColor Gray
} else {
    Write-Host "  Criando zip e deployando Lambda..."
    # Cria zip
    Compress-Archive -Path "glue_jobs\silver\lambda_retry.py" -DestinationPath "glue_jobs\silver\lambda_retry.zip" -Force
    
    aws lambda create-function `
        --function-name gpcorp-extraction-retry `
        --runtime python3.12 `
        --handler lambda_retry.handler `
        --role $glueRole `
        --zip-file fileb://glue_jobs/silver/lambda_retry.zip `
        --timeout 60 `
        --memory-size 128 `
        --environment "Variables={MAX_RETRIES=3,RETRY_QUEUE_URL=$retryUrl,DLQ_URL=$dlqUrl,ALERT_TOPIC_ARN=$SNS_TOPIC}" `
        --output text 2>$null
    Write-Host "  OK: gpcorp-extraction-retry criada" -ForegroundColor Green

    # Event source mapping: SQS -> Lambda
    $retryArn = aws sqs get-queue-attributes --queue-url $retryUrl --attribute-names QueueArn --query "Attributes.QueueArn" --output text 2>$null
    aws lambda create-event-source-mapping `
        --function-name gpcorp-extraction-retry `
        --event-source-arn $retryArn `
        --batch-size 1 `
        --enabled 2>$null
    Write-Host "  OK: SQS trigger configurado" -ForegroundColor Green
}

# ═══════════════════════════════════════════════════════════════
# Resumo
# ═══════════════════════════════════════════════════════════════

Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host "  Deploy concluido!" -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  SQS:" -ForegroundColor White
Write-Host "    - gpcorp-extraction-retry (backoff + redrive para DLQ)"
Write-Host "    - gpcorp-extraction-dlq (falhas permanentes)"
Write-Host ""
Write-Host "  Alarmes CloudWatch:" -ForegroundColor White
Write-Host "    - gpcorp-extraction-dlq-not-empty"
Write-Host "    - gpcorp-pipeline-sfn-failed"
Write-Host "    - gpcorp-pipeline-not-executed"
Write-Host "    - gpcorp-gold-estoque-failure"
Write-Host "    - gpcorp-gold-cadastros-failure"
Write-Host "    - gpcorp-silver-compaction-failure"
Write-Host ""
Write-Host "  IAM:" -ForegroundColor White
Write-Host "    - AnalystRole-gpcorp (QuickSight - read Gold + Silver)"
Write-Host ""
Write-Host "  Lake Formation:" -ForegroundColor White
Write-Host "    - GlueServiceRole: admin"
Write-Host "    - AnalystRole: SELECT Gold + Silver"
Write-Host ""
Write-Host "  Lambda:" -ForegroundColor White
Write-Host "    - gpcorp-extraction-retry (consome retry queue)"
Write-Host ""
