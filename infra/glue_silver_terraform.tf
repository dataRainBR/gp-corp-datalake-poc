# =============================================================================
# Terraform — AWS Glue Jobs Bronze → Silver (POC GP Corp)
# Otimizado para custo: G.1X workers, auto-scaling, timeout curto.
# =============================================================================

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

# --- Variables ---

variable "s3_bucket" {
  default = "gpcorp-datalake"
}

variable "glue_scripts_prefix" {
  default = "glue-scripts/silver"
}

variable "glue_role_arn" {
  description = "ARN da IAM Role para execução dos Glue Jobs"
  type        = string
}

# --- Glue Database (Silver) ---

resource "aws_glue_catalog_database" "silver" {
  name        = "gpcorp_silver"
  description = "Camada Silver — dados limpos, tipados e deduplicados do SAP B1"

  create_table_default_permission {
    permissions = ["ALL"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

# --- Glue Job: Dimensões (SCD2) ---

resource "aws_glue_job" "silver_dimensions" {
  name     = "gpcorp-silver-dimensions"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket}/${var.glue_scripts_prefix}/job_dimensions.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-auto-scaling"                = "true"
    "--enable-continuous-cloudwatch-log"    = "false"
    "--enable-metrics"                     = "true"
    "--datalake-formats"                   = "iceberg"
    "--conf"                               = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--extra-py-files"                     = "s3://${var.s3_bucket}/${var.glue_scripts_prefix}/config.py,s3://${var.s3_bucket}/${var.glue_scripts_prefix}/utils.py,s3://${var.s3_bucket}/${var.glue_scripts_prefix}/scd2.py,s3://${var.s3_bucket}/${var.glue_scripts_prefix}/iceberg_writer.py"
    "--entities"                           = "BusinessPartners,Items,ItemGroups,SalesPersons"
    "--TempDir"                            = "s3://${var.s3_bucket}/glue-temp/"
    "--job-bookmark-option"                = "job-bookmark-disable"
  }

  tags = {
    Project     = "gpcorp-datalake"
    Layer       = "silver"
    CostCenter  = "data-engineering"
  }
}

# --- Glue Job: Fatos (Transacionais) ---

resource "aws_glue_job" "silver_facts" {
  name     = "gpcorp-silver-facts"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  timeout           = 120
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket}/${var.glue_scripts_prefix}/job_facts.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-auto-scaling"                = "true"
    "--enable-continuous-cloudwatch-log"    = "false"
    "--enable-metrics"                     = "true"
    "--datalake-formats"                   = "iceberg"
    "--conf"                               = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--extra-py-files"                     = "s3://${var.s3_bucket}/${var.glue_scripts_prefix}/config.py,s3://${var.s3_bucket}/${var.glue_scripts_prefix}/utils.py,s3://${var.s3_bucket}/${var.glue_scripts_prefix}/iceberg_writer.py"
    "--entities"                           = "Invoices,Orders,Quotations,InventoryGenEntries"
    "--TempDir"                            = "s3://${var.s3_bucket}/glue-temp/"
    "--job-bookmark-option"                = "job-bookmark-disable"
  }

  tags = {
    Project     = "gpcorp-datalake"
    Layer       = "silver"
    CostCenter  = "data-engineering"
  }
}

# --- Glue Job: Silver Quality Checks (dbt tests via Athena) ---
# Python Shell: ~$0.01/run (1/16 DPU × ~5 min) + Athena scan (~$0.05)
# vs Spark anterior: ~$1.10/run (3 G.1X × 50 min)

resource "aws_glue_job" "silver_quality" {
  name     = "gpcorp-silver-quality-checks"
  role_arn = var.glue_role_arn

  command {
    name            = "pythonshell"
    script_location = "s3://${var.s3_bucket}/glue-scripts/dbt/run_dbt_tests.py"
    python_version  = "3.9"
  }

  max_capacity = 0.0625  # 1/16 DPU — mínimo para Python Shell
  timeout      = 30
  max_retries  = 1

  default_arguments = {
    "--additional-python-modules" = "dbt-athena-community==1.8.*,elementary-data==0.15.*"
    "--dbt_project_s3"           = "s3://${var.s3_bucket}/dbt/gpcorp_quality/"
    "--test_selector"            = "source:gpcorp_silver+"
    "--TempDir"                  = "s3://${var.s3_bucket}/glue-temp/"
  }

  tags = {
    Project     = "gpcorp-datalake"
    Layer       = "silver"
    CostCenter  = "data-engineering"
  }
}

# --- Glue Workflow (orquestração sequencial) ---

resource "aws_glue_workflow" "silver_pipeline" {
  name        = "gpcorp-pipeline"
  description = "Pipeline completo: Silver (Dims → Facts → QC) → Gold (Dashboards → Features/Estoque → QC)"

  tags = {
    Project = "gpcorp-datalake"
  }
}

# Trigger: Start — DESATIVADO (orquestração migrou para Step Functions)
# Mantido como ON_DEMAND para execuções manuais de teste
resource "aws_glue_trigger" "silver_start" {
  name          = "gpcorp-silver-start-on-demand"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.silver_pipeline.name

  actions {
    job_name = aws_glue_job.silver_dimensions.name
  }

  tags = {
    Project = "gpcorp-datalake"
  }
}

# Trigger: Fatos após Dimensões
resource "aws_glue_trigger" "silver_facts_after_dims" {
  name          = "gpcorp-silver-facts-after-dims"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.silver_pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.silver_dimensions.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.silver_facts.name
  }
}

# Trigger: Quality Checks após Fatos
resource "aws_glue_trigger" "silver_quality_after_facts" {
  name          = "gpcorp-silver-quality-after-facts"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.silver_pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.silver_facts.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.silver_quality.name
  }
}

# --- Outputs ---

output "silver_database_name" {
  value = aws_glue_catalog_database.silver.name
}

output "workflow_name" {
  value = aws_glue_workflow.silver_pipeline.name
}
