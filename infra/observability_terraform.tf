# =============================================================================
# Terraform — Observabilidade (CloudWatch Alarms + SNS)
# Monitora falhas nos Glue Jobs e tempos de execução.
# =============================================================================

# --- SNS Topic para alertas ---

resource "aws_sns_topic" "glue_alerts" {
  name = "gpcorp-glue-pipeline-alerts"

  tags = {
    Project = "gpcorp-datalake"
  }
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.glue_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

variable "alert_email" {
  description = "E-mail para receber alertas de falhas no pipeline"
  type        = string
}

# --- Alarme: Workflow falhou ---

resource "aws_cloudwatch_metric_alarm" "workflow_failure" {
  alarm_name          = "gpcorp-silver-pipeline-failure"
  alarm_description   = "Pipeline Silver falhou — verificar logs dos jobs"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = "gpcorp-silver-dimensions"
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]
  ok_actions    = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}

# --- Alarme por job (um para cada) ---

locals {
  monitored_jobs = [
    "gpcorp-silver-dimensions",
    "gpcorp-silver-facts",
    "gpcorp-silver-quality-checks",
    "gpcorp-gold-dashboards",
    "gpcorp-gold-features",
    "gpcorp-gold-estoque",
    "gpcorp-gold-quality-checks",
  ]
}

resource "aws_cloudwatch_metric_alarm" "job_failure" {
  for_each = toset(local.monitored_jobs)

  alarm_name          = "${each.value}-failure"
  alarm_description   = "Job ${each.value} falhou"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = each.value
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}

# --- Alarme: Tempo de execução excessivo (SLA breach) ---

resource "aws_cloudwatch_metric_alarm" "pipeline_duration" {
  alarm_name          = "gpcorp-pipeline-duration-sla"
  alarm_description   = "Pipeline total excedeu 2h (SLA: dados até 7h, janela 00h-05h)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.elapsedTime"
  namespace           = "Glue"
  period              = 900
  statistic           = "Maximum"
  threshold           = 7200000  # 2h em ms
  treat_missing_data  = "notBreaching"

  dimensions = {
    JobName = "gpcorp-silver-facts"
  }

  alarm_actions = [aws_sns_topic.glue_alerts.arn]

  tags = {
    Project = "gpcorp-datalake"
  }
}

# --- Dashboard CloudWatch ---

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "gpcorp-lakehouse-pipeline"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Job Duration (seconds)"
          metrics = [
            for job in local.monitored_jobs :
            ["Glue", "glue.driver.aggregate.elapsedTime", "JobName", job]
          ]
          period = 86400
          stat   = "Maximum"
          region = "us-east-1"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Records Processed"
          metrics = [
            for job in local.monitored_jobs :
            ["Glue", "glue.driver.aggregate.recordsRead", "JobName", job]
          ]
          period = 86400
          stat   = "Sum"
          region = "us-east-1"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 24
        height = 6
        properties = {
          title   = "Failed Tasks"
          metrics = [
            for job in local.monitored_jobs :
            ["Glue", "glue.driver.aggregate.numFailedTasks", "JobName", job]
          ]
          period = 86400
          stat   = "Sum"
          region = "us-east-1"
        }
      }
    ]
  })
}
