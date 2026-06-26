# =============================================================================
# Terraform — Lambda Trigger + EventBridge + CloudWatch Alerts
# Automação do pipeline Silver com monitoramento de falhas
# =============================================================================

# --- Lambda Packages (gerados automaticamente) ---

data "archive_file" "lambda_trigger" {
  type        = "zip"
  source_file = "${path.module}/../glue_jobs/silver/lambda_trigger.py"
  output_path = "${path.module}/../glue_jobs/silver/lambda_trigger.zip"
}

data "archive_file" "lambda_retry" {
  type        = "zip"
  source_file = "${path.module}/../glue_jobs/silver/lambda_retry.py"
  output_path = "${path.module}/../glue_jobs/silver/lambda_retry.zip"
}

# --- Lambda Function: Trigger Silver Pipeline ---

resource "aws_lambda_function" "silver_trigger" {
  function_name = "gpcorp-silver-trigger"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "lambda_trigger.handler"
  runtime       = "python3.12"
  timeout       = 30
  memory_size   = 128

  filename         = data.archive_file.lambda_trigger.output_path
  source_code_hash = data.archive_file.lambda_trigger.output_base64sha256

  environment {
    variables = {
      GLUE_WORKFLOW_NAME = "gpcorp-pipeline"
    }
  }

  tags = {
    Project    = "gpcorp-datalake"
    Layer      = "orchestration"
    CostCenter = "data-engineering"
  }
}

# --- Lambda Function: Retry Handler (DLQ consumer) ---

resource "aws_lambda_function" "extraction_retry" {
  function_name = "gpcorp-extraction-retry"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "lambda_retry.handler"
  runtime       = "python3.12"
  timeout       = 60
  memory_size   = 128

  filename         = data.archive_file.lambda_retry.output_path
  source_code_hash = data.archive_file.lambda_retry.output_base64sha256

  environment {
    variables = {
      MAX_RETRIES     = "3"
      RETRY_QUEUE_URL = aws_sqs_queue.extraction_retry.url
      DLQ_URL         = aws_sqs_queue.extraction_dlq.url
      ALERT_TOPIC_ARN = aws_sns_topic.glue_alerts.arn
    }
  }

  tags = {
    Project    = "gpcorp-datalake"
    Layer      = "orchestration"
    CostCenter = "data-engineering"
  }
}

# --- EventBridge: Schedule diário — DESABILITADO (migrado para Step Functions) ---
# Mantido para rollback caso necessário

resource "aws_cloudwatch_event_rule" "silver_daily" {
  name                = "gpcorp-silver-daily"
  description         = "DESABILITADO — orquestração migrou para Step Functions"
  schedule_expression = "cron(0 2 * * ? *)"
  state               = "DISABLED"

  tags = {
    Project = "gpcorp-datalake"
  }
}

resource "aws_cloudwatch_event_target" "silver_trigger" {
  rule      = aws_cloudwatch_event_rule.silver_daily.name
  target_id = "silver-trigger-lambda"
  arn       = aws_lambda_function.silver_trigger.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.silver_trigger.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.silver_daily.arn
}

# --- EventBridge: Retry consumer a cada 5 minutos ---

resource "aws_lambda_event_source_mapping" "retry_sqs" {
  event_source_arn = aws_sqs_queue.extraction_retry.arn
  function_name    = aws_lambda_function.extraction_retry.arn
  batch_size       = 1
  enabled          = true
}

# =============================================================================
# CloudWatch Alarms — Lambda Failures
# =============================================================================

resource "aws_cloudwatch_metric_alarm" "lambda_trigger_errors" {
  alarm_name          = "gpcorp-lambda-silver-trigger-errors"
  alarm_description   = "Lambda silver-trigger está falhando"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.silver_trigger.function_name
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]
  ok_actions    = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_retry_errors" {
  alarm_name          = "gpcorp-lambda-extraction-retry-errors"
  alarm_description   = "Lambda extraction-retry está falhando"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.extraction_retry.function_name
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}

# --- Alarme: Lambda Throttled ---

resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  alarm_name          = "gpcorp-lambda-throttles"
  alarm_description   = "Lambdas estão sendo throttled — verificar limits"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Throttles"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.silver_trigger.function_name
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}

# --- Alarme: Workflow não iniciou (Lambda invocação = 0 no horário esperado) ---

resource "aws_cloudwatch_metric_alarm" "workflow_not_started" {
  alarm_name          = "gpcorp-workflow-not-started"
  alarm_description   = "Pipeline não foi disparado no horário esperado (02:00 UTC)"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Invocations"
  namespace           = "AWS/Lambda"
  period              = 3600  # 1h window
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "breaching"

  dimensions = {
    FunctionName = aws_lambda_function.silver_trigger.function_name
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}
