# =============================================================================
# Terraform — IAM Roles (Least Privilege)
# Roles separadas por função: Glue ETL, Lambda, Analista (QuickSight)
# Otimizado: sem over-provisioning de permissões
# =============================================================================

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

# =============================================================================
# Role 1: Glue ETL — executa jobs Silver/Gold (Spark + Iceberg)
# =============================================================================

resource "aws_iam_role" "glue_etl" {
  name = "GlueETLRole-gpcorp"
  path = "/service-role/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = {
    Project    = "gpcorp-datalake"
    CostCenter = "data-engineering"
  }
}

# Glue Service (mínimo para rodar jobs)
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_etl.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# S3: leitura Bronze, leitura/escrita Silver+Gold, temp
resource "aws_iam_role_policy" "glue_s3" {
  name = "gpcorp-glue-s3-access"
  role = aws_iam_role.glue_etl.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadBronze"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/Bronze/*",
        ]
      },
      {
        Sid    = "ReadWriteSilverGold"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}/Silver/*",
          "arn:aws:s3:::${var.s3_bucket}/Gold/*",
          "arn:aws:s3:::${var.s3_bucket}/silver/*",
          "arn:aws:s3:::${var.s3_bucket}/glue-temp/*",
          "arn:aws:s3:::${var.s3_bucket}/glue-scripts/*",
        ]
      },
    ]
  })
}

# Glue Catalog: CRUD em tabelas Silver/Gold
resource "aws_iam_role_policy" "glue_catalog" {
  name = "gpcorp-glue-catalog-access"
  role = aws_iam_role.glue_etl.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
        ]
        Resource = [
          "arn:aws:glue:${local.region}:${local.account_id}:catalog",
          "arn:aws:glue:${local.region}:${local.account_id}:database/gpcorp_*",
          "arn:aws:glue:${local.region}:${local.account_id}:table/gpcorp_*/*",
        ]
      },
    ]
  })
}

# Lake Formation: permite acesso via LF
resource "aws_iam_role_policy" "glue_lakeformation" {
  name = "gpcorp-glue-lakeformation"
  role = aws_iam_role.glue_etl.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "lakeformation:GetDataAccess",
      ]
      Resource = "*"
    }]
  })
}

# CloudWatch Logs (para métricas e debugging)
resource "aws_iam_role_policy" "glue_logs" {
  name = "gpcorp-glue-cloudwatch-logs"
  role = aws_iam_role.glue_etl.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
      ]
      Resource = "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"
    }]
  })
}

# =============================================================================
# Role 2: Lambda Orchestration — trigger pipeline + retry
# Mínimo: start Glue jobs, SQS, SNS, CloudWatch Logs
# =============================================================================

resource "aws_iam_role" "lambda_execution" {
  name = "LambdaOrchRole-gpcorp"
  path = "/service-role/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = {
    Project    = "gpcorp-datalake"
    CostCenter = "data-engineering"
  }
}

# Lambda básico (CloudWatch Logs)
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Glue: apenas start job run (não precisa mais que isso)
resource "aws_iam_role_policy" "lambda_glue" {
  name = "gpcorp-lambda-glue-start"
  role = aws_iam_role.lambda_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:StartWorkflowRun",
      ]
      Resource = [
        "arn:aws:glue:${local.region}:${local.account_id}:job/gpcorp-*",
        "arn:aws:glue:${local.region}:${local.account_id}:workflow/gpcorp-*",
      ]
    }]
  })
}

# SQS: enviar/receber mensagens na retry queue
resource "aws_iam_role_policy" "lambda_sqs" {
  name = "gpcorp-lambda-sqs"
  role = aws_iam_role.lambda_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
      ]
      Resource = [
        "arn:aws:sqs:${local.region}:${local.account_id}:gpcorp-extraction-*",
      ]
    }]
  })
}

# SNS: publicar alertas
resource "aws_iam_role_policy" "lambda_sns" {
  name = "gpcorp-lambda-sns"
  role = aws_iam_role.lambda_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["sns:Publish"]
      Resource = ["arn:aws:sns:${local.region}:${local.account_id}:gpcorp-*"]
    }]
  })
}

# =============================================================================
# Role 3: Analista (QuickSight / Athena) — somente leitura Gold
# =============================================================================

resource "aws_iam_role" "analyst" {
  name = "AnalystRole-gpcorp"
  path = "/"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "quicksight.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })

  tags = {
    Project    = "gpcorp-datalake"
    CostCenter = "analytics"
  }
}

# Athena query execution
resource "aws_iam_role_policy" "analyst_athena" {
  name = "gpcorp-analyst-athena"
  role = aws_iam_role.analyst.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
        ]
        Resource = "arn:aws:athena:${local.region}:${local.account_id}:workgroup/primary"
      },
      {
        Sid    = "AthenaResults"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}/athena-results/*",
          "arn:aws:s3:::${var.s3_bucket}",
        ]
      },
      {
        Sid    = "ReadGoldSilverData"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}/Gold/*",
          "arn:aws:s3:::${var.s3_bucket}/Silver/*",
        ]
      },
    ]
  })
}

# Glue Catalog: read-only para Gold/Silver
resource "aws_iam_role_policy" "analyst_catalog" {
  name = "gpcorp-analyst-catalog"
  role = aws_iam_role.analyst.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartitions",
      ]
      Resource = [
        "arn:aws:glue:${local.region}:${local.account_id}:catalog",
        "arn:aws:glue:${local.region}:${local.account_id}:database/gpcorp_gold_*",
        "arn:aws:glue:${local.region}:${local.account_id}:database/gpcorp_silver",
        "arn:aws:glue:${local.region}:${local.account_id}:table/gpcorp_gold_*/*",
        "arn:aws:glue:${local.region}:${local.account_id}:table/gpcorp_silver/*",
      ]
    }]
  })
}

# Lake Formation
resource "aws_iam_role_policy" "analyst_lakeformation" {
  name = "gpcorp-analyst-lakeformation"
  role = aws_iam_role.analyst.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["lakeformation:GetDataAccess"]
      Resource = "*"
    }]
  })
}

# =============================================================================
# Outputs
# =============================================================================

output "glue_etl_role_arn" {
  value       = aws_iam_role.glue_etl.arn
  description = "ARN da role Glue ETL (usar no glue_role_arn)"
}

output "lambda_execution_role_arn" {
  value       = aws_iam_role.lambda_execution.arn
  description = "ARN da role Lambda orchestration"
}

output "analyst_role_arn" {
  value       = aws_iam_role.analyst.arn
  description = "ARN da role Analista (QuickSight)"
}
