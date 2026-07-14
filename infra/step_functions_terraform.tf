# =============================================================================
# Terraform — Step Functions Pipeline (orquestrador mestre)
# Substitui: Glue Workflow + Lambda trigger + EventBridge separados
# Fluxo: Bronze (API) → Crawler → Silver → Gold → Quality → SNS
# =============================================================================

# --- Step Functions State Machine ---

resource "aws_sfn_state_machine" "pipeline" {
  name     = "GPCorp_data_lake_pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = file("${path.module}/step_functions_pipeline.json")

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_logs.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tags = {
    Project    = "gpcorp-datalake"
    CostCenter = "data-engineering"
  }
}

# --- CloudWatch Log Group para Step Functions ---

resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/aws/stepfunctions/GPCorp_data_lake_pipeline"
  retention_in_days = 30

  tags = {
    Project = "gpcorp-datalake"
  }
}

# --- EventBridge: Schedule diário 00:00 UTC (21:00 BRT) ---
# Antecipado para dar tempo ao pipeline completo terminar antes das 7h

resource "aws_cloudwatch_event_rule" "pipeline_daily" {
  name                = "gpcorp-pipeline-daily"
  description         = "Dispara pipeline completo diariamente (extração + transformação)"
  schedule_expression = "cron(0 0 * * ? *)"

  tags = {
    Project = "gpcorp-datalake"
  }
}

resource "aws_cloudwatch_event_target" "pipeline_sfn" {
  rule      = aws_cloudwatch_event_rule.pipeline_daily.name
  target_id = "pipeline-step-functions"
  arn       = aws_sfn_state_machine.pipeline.arn
  role_arn  = aws_iam_role.eventbridge_sfn.arn
}

# --- IAM Role: Step Functions ---

resource "aws_iam_role" "step_functions" {
  name = "StepFunctionsRole-gpcorp-pipeline"
  path = "/service-role/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })

  tags = {
    Project = "gpcorp-datalake"
  }
}

# Step Functions precisa: invocar Lambda, start Glue jobs, start Crawler, publicar SNS, logs
resource "aws_iam_role_policy" "sfn_policy" {
  name = "gpcorp-sfn-pipeline-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "InvokeLambdas"
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = [
          "arn:aws:lambda:${local.region}:${local.account_id}:function:gp_corp_*"
        ]
      },
      {
        Sid    = "GlueJobs"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:job/gpcorp-*"
        ]
      },
      {
        Sid    = "GlueCrawler"
        Effect = "Allow"
        Action = [
          "glue:StartCrawler",
          "glue:GetCrawler"
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:crawler/gpcorp-*"
        ]
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = [aws_sns_topic.glue_alerts.arn]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:CreateLogStream",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutLogEvents",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

# --- IAM Role: EventBridge → Step Functions ---

resource "aws_iam_role" "eventbridge_sfn" {
  name = "EventBridgeRole-gpcorp-sfn"
  path = "/service-role/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })

  tags = {
    Project = "gpcorp-datalake"
  }
}

resource "aws_iam_role_policy" "eventbridge_start_sfn" {
  name = "gpcorp-eventbridge-start-sfn"
  role = aws_iam_role.eventbridge_sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.pipeline.arn
    }]
  })
}

# --- Alarme: Pipeline não executou hoje ---

resource "aws_cloudwatch_metric_alarm" "sfn_not_executed" {
  alarm_name          = "gpcorp-pipeline-not-executed"
  alarm_description   = "Step Functions pipeline não executou nas últimas 26h"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsStarted"
  namespace           = "AWS/States"
  period              = 93600  # 26h
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "breaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline.arn
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}

# --- Alarme: Pipeline falhou ---

resource "aws_cloudwatch_metric_alarm" "sfn_failed" {
  alarm_name          = "gpcorp-pipeline-failed"
  alarm_description   = "Step Functions pipeline falhou"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 86400
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline.arn
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}

# --- Output ---

output "step_functions_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}
