# =============================================================================
# Terraform — Glue Crawler + Database para camada Bronze
# Cataloga JSONs raw do SAP B1 para consulta ad-hoc via Athena
# =============================================================================

# --- Glue Database (Bronze) ---

resource "aws_glue_catalog_database" "bronze" {
  name        = "gpcorp_bronze"
  description = "Camada Bronze — dados raw do SAP B1 (JSON). Catalogado via Crawler."

  create_table_default_permission {
    permissions = ["ALL"]
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

# --- Crawler Bronze ---

resource "aws_glue_crawler" "bronze" {
  name          = "gpcorp-bronze-crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.bronze.name
  description   = "Cataloga entidades Bronze (SAP B1 JSON) para consulta Athena"

  schedule = "cron(30 5 * * ? *)" # 05:30 UTC = 02:30 BRT (após extração)

  s3_target {
    path = "s3://${var.s3_bucket}/Bronze/BusinessPartners/"
  }

  s3_target {
    path = "s3://${var.s3_bucket}/Bronze/Items/"
  }

  s3_target {
    path = "s3://${var.s3_bucket}/Bronze/ItemGroups/"
  }

  s3_target {
    path = "s3://${var.s3_bucket}/Bronze/SalesPersons/"
  }

  s3_target {
    path = "s3://${var.s3_bucket}/Bronze/Invoices/"
  }

  s3_target {
    path = "s3://${var.s3_bucket}/Bronze/Orders/"
  }

  s3_target {
    path = "s3://${var.s3_bucket}/Bronze/Quotations/"
  }

  s3_target {
    path = "s3://${var.s3_bucket}/Bronze/InventoryGenEntries/"
  }

  schema_change_policy {
    update_behavior = "LOG"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })

  recrawl_policy {
    recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY"
  }

  tags = {
    Project    = "gpcorp-lakehouse-poc"
    Layer      = "bronze"
    CostCenter = "data-engineering"
  }
}

# --- Output ---

output "bronze_database_name" {
  value = aws_glue_catalog_database.bronze.name
}

output "bronze_crawler_name" {
  value = aws_glue_crawler.bronze.name
}
