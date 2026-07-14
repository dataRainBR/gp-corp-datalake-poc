# =============================================================================
# Terraform — SQS Retry Queue + DLQ para falhas de extração Bronze
# Padrão: retry com backoff exponencial → DLQ após N tentativas
# =============================================================================

# --- Dead Letter Queue (falhas permanentes) ---

resource "aws_sqs_queue" "extraction_dlq" {
  name                      = "gpcorp-extraction-dlq"
  message_retention_seconds = 1209600  # 14 dias
  visibility_timeout_seconds = 60

  tags = {
    Project    = "gpcorp-datalake"
    Layer      = "bronze"
    CostCenter = "data-engineering"
  }
}

# --- Retry Queue (falhas temporárias com backoff) ---

resource "aws_sqs_queue" "extraction_retry" {
  name                       = "gpcorp-extraction-retry"
  delay_seconds              = 60          # Delay inicial de 1 min
  message_retention_seconds  = 86400       # 24h
  visibility_timeout_seconds = 120         # 2 min para processar
  receive_wait_time_seconds  = 20          # Long polling

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.extraction_dlq.arn
    maxReceiveCount     = 3  # Após 3 tentativas vai para DLQ
  })

  tags = {
    Project    = "gpcorp-datalake"
    Layer      = "bronze"
    CostCenter = "data-engineering"
  }
}

# --- Alarme: Mensagens na DLQ (falhas permanentes requerem atenção) ---

resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  alarm_name          = "gpcorp-extraction-dlq-not-empty"
  alarm_description   = "Existem extrações que falharam permanentemente na DLQ — intervenção manual necessária"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.extraction_dlq.name
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}

# --- Alarme: Muitas mensagens na retry queue (possível problema sistêmico) ---

resource "aws_cloudwatch_metric_alarm" "retry_queue_backlog" {
  alarm_name          = "gpcorp-extraction-retry-backlog"
  alarm_description   = "Muitas extrações em retry — possível falha no SAP ou conectividade"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 10
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.extraction_retry.name
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}

# --- Outputs ---

output "retry_queue_url" {
  value = aws_sqs_queue.extraction_retry.url
}

output "dlq_url" {
  value = aws_sqs_queue.extraction_dlq.url
}

output "retry_queue_arn" {
  value = aws_sqs_queue.extraction_retry.arn
}

output "dlq_arn" {
  value = aws_sqs_queue.extraction_dlq.arn
}
