# =============================================================================
# Test Pipeline — Verifica e testa o workflow completo via AWS CLI
# Executa em PowerShell com AWS CLI configurado
# =============================================================================

param(
    [string]$Action = "status",   # status | start | test-lambda | test-retry | full
    [string]$LoadType = "incremental",
    [string]$LoadDate = (Get-Date -Format "yyyy-MM-dd")
)

$ErrorActionPreference = "Stop"
$WORKFLOW_NAME = "gpcorp-pipeline"
$LAMBDA_TRIGGER = "gpcorp-silver-trigger"
$LAMBDA_RETRY = "gpcorp-extraction-retry"
$RETRY_QUEUE = "gpcorp-extraction-retry"

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  GP Corp Datalake — Pipeline Test CLI" -ForegroundColor Cyan
Write-Host "  Action: $Action | LoadDate: $LoadDate | LoadType: $LoadType" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

# ─── Helper Functions ───

function Get-WorkflowStatus {
    Write-Host "[WORKFLOW] Status do workflow '$WORKFLOW_NAME':" -ForegroundColor Yellow
    
    try {
        $runs = aws glue get-workflow-runs --name $WORKFLOW_NAME --max-results 3 --output json | ConvertFrom-Json
        
        if ($runs.Runs.Count -eq 0) {
            Write-Host "  Nenhuma execucao encontrada" -ForegroundColor Gray
            return
        }
        
        foreach ($run in $runs.Runs) {
            $status = $run.Status
            $startedOn = $run.StartedOn
            $color = switch ($status) {
                "COMPLETED" { "Green" }
                "RUNNING"   { "Yellow" }
                "STOPPED"   { "Red" }
                "ERROR"     { "Red" }
                default     { "Gray" }
            }
            Write-Host "  RunId: $($run.RunId) | Status: $status | Started: $startedOn" -ForegroundColor $color
        }
    }
    catch {
        Write-Host "  Erro ao consultar workflow: $($_.Exception.Message)" -ForegroundColor Red
    }
}

function Get-JobsStatus {
    Write-Host ""
    Write-Host "[JOBS] Status dos Glue Jobs:" -ForegroundColor Yellow
    
    $jobs = @(
        "gpcorp-silver-dimensions",
        "gpcorp-silver-facts",
        "gpcorp-silver-quality-checks",
        "gpcorp-gold-dashboards",
        "gpcorp-gold-features",
        "gpcorp-gold-estoque",
        "gpcorp-gold-quality-checks"
    )
    
    foreach ($job in $jobs) {
        try {
            $result = aws glue get-job-runs --job-name $job --max-results 1 --output json 2>$null | ConvertFrom-Json
            if ($result.JobRuns.Count -gt 0) {
                $lastRun = $result.JobRuns[0]
                $status = $lastRun.JobRunState
                $duration = if ($lastRun.ExecutionTime) { "$($lastRun.ExecutionTime)s" } else { "N/A" }
                $color = switch ($status) {
                    "SUCCEEDED" { "Green" }
                    "RUNNING"   { "Yellow" }
                    "FAILED"    { "Red" }
                    "TIMEOUT"   { "Red" }
                    default     { "Gray" }
                }
                Write-Host "  $job : $status ($duration)" -ForegroundColor $color
            }
            else {
                Write-Host "  $job : nunca executado" -ForegroundColor Gray
            }
        }
        catch {
            Write-Host "  $job : nao encontrado" -ForegroundColor DarkGray
        }
    }
}

function Get-LambdaStatus {
    Write-Host ""
    Write-Host "[LAMBDA] Status das Lambdas:" -ForegroundColor Yellow
    
    $lambdas = @($LAMBDA_TRIGGER, $LAMBDA_RETRY)
    
    foreach ($fn in $lambdas) {
        try {
            $config = aws lambda get-function --function-name $fn --output json 2>$null | ConvertFrom-Json
            $state = $config.Configuration.State
            $lastModified = $config.Configuration.LastModified
            Write-Host "  $fn : $state (modified: $lastModified)" -ForegroundColor Green
        }
        catch {
            Write-Host "  $fn : NAO ENCONTRADA (deploy pendente)" -ForegroundColor Red
        }
    }
}

function Get-QueueStatus {
    Write-Host ""
    Write-Host "[SQS] Status das filas:" -ForegroundColor Yellow
    
    $queues = @("gpcorp-extraction-retry", "gpcorp-extraction-dlq")
    
    foreach ($q in $queues) {
        try {
            $url = aws sqs get-queue-url --queue-name $q --output text 2>$null
            $attrs = aws sqs get-queue-attributes --queue-url $url --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible --output json 2>$null | ConvertFrom-Json
            $visible = $attrs.Attributes.ApproximateNumberOfMessages
            $inflight = $attrs.Attributes.ApproximateNumberOfMessagesNotVisible
            Write-Host "  $q : $visible mensagens (in-flight: $inflight)" -ForegroundColor Green
        }
        catch {
            Write-Host "  $q : NAO ENCONTRADA (deploy pendente)" -ForegroundColor Red
        }
    }
}

function Get-AlarmsStatus {
    Write-Host ""
    Write-Host "[ALARMS] CloudWatch Alarms:" -ForegroundColor Yellow
    
    try {
        $alarms = aws cloudwatch describe-alarms --alarm-name-prefix "gpcorp-" --output json | ConvertFrom-Json
        
        foreach ($alarm in $alarms.MetricAlarms) {
            $state = $alarm.StateValue
            $color = switch ($state) {
                "OK"                { "Green" }
                "ALARM"             { "Red" }
                "INSUFFICIENT_DATA" { "Yellow" }
                default             { "Gray" }
            }
            Write-Host "  $($alarm.AlarmName) : $state" -ForegroundColor $color
        }
    }
    catch {
        Write-Host "  Erro ao consultar alarms: $($_.Exception.Message)" -ForegroundColor Red
    }
}

function Start-Workflow {
    Write-Host "[START] Iniciando workflow '$WORKFLOW_NAME'..." -ForegroundColor Green
    Write-Host "  load_type=$LoadType, load_date=$LoadDate" -ForegroundColor Gray
    
    try {
        $result = aws glue start-workflow-run `
            --name $WORKFLOW_NAME `
            --run-properties "{`"load_type`":`"$LoadType`",`"load_date`":`"$LoadDate`"}" `
            --output json | ConvertFrom-Json
        
        Write-Host "  Workflow iniciado! RunId: $($result.RunId)" -ForegroundColor Green
        Write-Host ""
        Write-Host "  Monitorar:" -ForegroundColor Gray
        Write-Host "    aws glue get-workflow-run --name $WORKFLOW_NAME --run-id $($result.RunId)" -ForegroundColor Gray
    }
    catch {
        Write-Host "  ERRO: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "  Verificar: workflow existe? IAM tem permissao?" -ForegroundColor Yellow
    }
}

function Test-Lambda {
    Write-Host "[TEST-LAMBDA] Invocando '$LAMBDA_TRIGGER' (dry-run)..." -ForegroundColor Green
    
    try {
        $payload = @{ source = "cli-test"; detail = @{} } | ConvertTo-Json -Compress
        
        $result = aws lambda invoke `
            --function-name $LAMBDA_TRIGGER `
            --payload $payload `
            --output json `
            /tmp/lambda_response.json
        
        $response = Get-Content /tmp/lambda_response.json | ConvertFrom-Json
        Write-Host "  Status: $($result.StatusCode)" -ForegroundColor Green
        Write-Host "  Response: $($response | ConvertTo-Json -Depth 3)" -ForegroundColor Gray
    }
    catch {
        Write-Host "  ERRO: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "  Lambda pode nao estar deployada ainda" -ForegroundColor Yellow
    }
}

function Test-RetryQueue {
    Write-Host "[TEST-RETRY] Enviando mensagem de teste para retry queue..." -ForegroundColor Green
    
    $testMessage = @{
        entity    = "Invoices"
        load_date = $LoadDate
        attempt   = 1
        error     = "CLI test - connection timeout"
        source    = "cli_test"
    } | ConvertTo-Json -Compress
    
    try {
        $url = aws sqs get-queue-url --queue-name $RETRY_QUEUE --output text
        
        aws sqs send-message `
            --queue-url $url `
            --message-body $testMessage `
            --delay-seconds 0
        
        Write-Host "  Mensagem enviada para $RETRY_QUEUE" -ForegroundColor Green
        Write-Host "  Payload: $testMessage" -ForegroundColor Gray
        Write-Host ""
        Write-Host "  A Lambda $LAMBDA_RETRY deve consumir em ate 1 min" -ForegroundColor Yellow
    }
    catch {
        Write-Host "  ERRO: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "  Fila pode nao estar criada (rodar terraform apply)" -ForegroundColor Yellow
    }
}

# ─── Main Switch ───

switch ($Action) {
    "status" {
        Get-WorkflowStatus
        Get-JobsStatus
        Get-LambdaStatus
        Get-QueueStatus
        Get-AlarmsStatus
    }
    "start" {
        Start-Workflow
    }
    "test-lambda" {
        Test-Lambda
    }
    "test-retry" {
        Test-RetryQueue
    }
    "full" {
        Write-Host "═══ FULL TEST ═══" -ForegroundColor Magenta
        Write-Host ""
        Get-WorkflowStatus
        Get-JobsStatus
        Get-LambdaStatus
        Get-QueueStatus
        Get-AlarmsStatus
        Write-Host ""
        Write-Host "═══════════════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host ""
        Start-Workflow
    }
    default {
        Write-Host "Uso: .\test_pipeline.ps1 -Action <status|start|test-lambda|test-retry|full>" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "  status      - Mostra status de todos os componentes"
        Write-Host "  start       - Inicia o workflow com parametros"
        Write-Host "  test-lambda - Invoca Lambda trigger manualmente"
        Write-Host "  test-retry  - Envia mensagem de teste para retry queue"
        Write-Host "  full        - Status + inicia workflow"
        Write-Host ""
        Write-Host "Parametros:" -ForegroundColor Yellow
        Write-Host "  -LoadType   incremental|full|all (default: incremental)"
        Write-Host "  -LoadDate   YYYY-MM-DD (default: hoje)"
    }
}

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
