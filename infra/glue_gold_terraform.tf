# =============================================================================
# Terraform — AWS Glue Jobs Silver → Gold (POC GP Corp)
# CU-01: Dashboards QuickSight | CU-02: Feature Table IA
# =============================================================================

# --- Glue Database (Gold) ---

resource "aws_glue_catalog_database" "gold" {
  name        = "gpcorp_gold"
  description = "Camada Gold — agregações para dashboards e feature tables ML"

  create_table_default_permission {
    permissions = ["ALL"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

# --- Glue Job: Dashboards (CU-01) ---

resource "aws_glue_job" "gold_dashboards" {
  name     = "gpcorp-gold-dashboards"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket}/glue-scripts/gold/job_dashboards.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-auto-scaling"             = "true"
    "--enable-continuous-cloudwatch-log" = "false"
    "--enable-metrics"                  = "true"
    "--datalake-formats"                = "iceberg"
    "--conf"                            = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--extra-py-files"                  = "s3://${var.s3_bucket}/glue-scripts/gold/config.py"
    "--TempDir"                         = "s3://${var.s3_bucket}/glue-temp/"
    "--job-bookmark-option"             = "job-bookmark-disable"
  }

  tags = {
    Project    = "gpcorp-datalake"
    Layer      = "gold"
    CostCenter = "data-engineering"
  }
}

# --- Glue Job: Feature Table (CU-02) ---

resource "aws_glue_job" "gold_features" {
  name     = "gpcorp-gold-features"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket}/glue-scripts/gold/job_features.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-auto-scaling"             = "true"
    "--enable-continuous-cloudwatch-log" = "false"
    "--enable-metrics"                  = "true"
    "--datalake-formats"                = "iceberg"
    "--conf"                            = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--extra-py-files"                  = "s3://${var.s3_bucket}/glue-scripts/gold/config.py"
    "--TempDir"                         = "s3://${var.s3_bucket}/glue-temp/"
    "--job-bookmark-option"             = "job-bookmark-disable"
  }

  tags = {
    Project    = "gpcorp-datalake"
    Layer      = "gold"
    CostCenter = "data-engineering"
  }
}

# --- Triggers: Gold após Silver Quality ---

resource "aws_glue_trigger" "gold_dashboards_after_quality" {
  name          = "gpcorp-gold-dashboards-after-quality"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.silver_pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.silver_quality.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.gold_dashboards.name
  }
}

resource "aws_glue_trigger" "gold_features_after_dashboards" {
  name          = "gpcorp-gold-features-after-dashboards"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.silver_pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.gold_dashboards.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.gold_features.name
  }
}

# --- Glue Job: Gold Estoque ---

resource "aws_glue_job" "gold_estoque" {
  name     = "gpcorp-gold-estoque"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket}/glue-scripts/gold/job_estoque.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-auto-scaling"             = "true"
    "--enable-continuous-cloudwatch-log" = "false"
    "--enable-metrics"                  = "true"
    "--datalake-formats"                = "iceberg"
    "--conf"                            = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--extra-py-files"                  = "s3://${var.s3_bucket}/glue-scripts/gold/config.py"
    "--TempDir"                         = "s3://${var.s3_bucket}/glue-temp/"
    "--job-bookmark-option"             = "job-bookmark-disable"
  }

  tags = {
    Project    = "gpcorp-datalake"
    Layer      = "gold"
    CostCenter = "data-engineering"
  }
}

# Trigger: Estoque roda em paralelo com Features (ambos após Dashboards)
resource "aws_glue_trigger" "gold_estoque_after_dashboards" {
  name          = "gpcorp-gold-estoque-after-dashboards"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.silver_pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.gold_dashboards.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.gold_estoque.name
  }
}

# --- Glue Job: Gold Quality Checks (dbt tests sobre Gold) ---
# Python Shell: ~$0.44/DPU-hora (vs $0.44/DPU-hora Spark com overhead maior)
# MaxDPUs 0.0625 (1/16 DPU) é suficiente para dbt tests via Athena

resource "aws_glue_job" "gold_quality" {
  name     = "gpcorp-gold-quality-checks"
  role_arn = var.glue_role_arn

  command {
    name            = "pythonshell"
    script_location = "s3://${var.s3_bucket}/glue-scripts/dbt/run_dbt_tests.py"
    python_version  = "3.9"
  }

  max_capacity = 0.0625  # 1/16 DPU — mínimo para Python Shell
  timeout      = 30
  max_retries  = 0

  default_arguments = {
    "--additional-python-modules" = "dbt-athena-community==1.8.*,elementary-data==0.15.*"
    "--dbt_project_s3"           = "s3://${var.s3_bucket}/dbt/gpcorp_quality/"
    "--test_selector"            = "source:gpcorp_gold_vendas+ source:gpcorp_gold_cotacoes+ source:gpcorp_gold_cadastros+ source:gpcorp_gold_estoque+"
    "--TempDir"                  = "s3://${var.s3_bucket}/glue-temp/"
  }

  tags = {
    Project    = "gpcorp-datalake"
    Layer      = "gold"
    CostCenter = "data-engineering"
  }
}

# Trigger: Quality Checks Gold após todos os Gold jobs concluírem
resource "aws_glue_trigger" "gold_quality_after_all" {
  name          = "gpcorp-gold-quality-after-all"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.silver_pipeline.name

  predicate {
    logical = "AND"

    conditions {
      job_name = aws_glue_job.gold_features.name
      state    = "SUCCEEDED"
    }

    conditions {
      job_name = aws_glue_job.gold_estoque.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.gold_quality.name
  }
}
