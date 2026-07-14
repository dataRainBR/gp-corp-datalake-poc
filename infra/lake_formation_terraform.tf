# =============================================================================
# Terraform — AWS Lake Formation RBAC
# 3 perfis: admin, engenheiro de dados, analista
# Referencia roles criadas em iam_roles_terraform.tf
# =============================================================================

# --- Lake Formation Settings ---

resource "aws_lakeformation_data_lake_settings" "main" {
  admins = [aws_iam_role.glue_etl.arn, var.admin_role_arn]
}

variable "admin_role_arn" {
  description = "ARN da IAM Role do administrador Lake Formation"
  type        = string
}

# --- Lake Formation Resource (registra localização S3) ---

resource "aws_lakeformation_resource" "bronze" {
  arn = "arn:aws:s3:::${var.s3_bucket}/Bronze"
}

resource "aws_lakeformation_resource" "silver" {
  arn = "arn:aws:s3:::${var.s3_bucket}/Silver"
}

resource "aws_lakeformation_resource" "gold" {
  arn = "arn:aws:s3:::${var.s3_bucket}/Gold"
}

# =============================================================================
# Perfil 1: Admin — acesso total a todas as camadas
# =============================================================================

resource "aws_lakeformation_permissions" "admin_bronze" {
  principal   = var.admin_role_arn
  permissions = ["ALL"]

  database {
    name = "gpcorp_bronze"
  }
}

resource "aws_lakeformation_permissions" "admin_silver" {
  principal   = var.admin_role_arn
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.silver.name
  }
}

resource "aws_lakeformation_permissions" "admin_gold" {
  principal   = var.admin_role_arn
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.gold.name
  }
}

# =============================================================================
# Perfil 2: Engenheiro de Dados (Glue ETL Role)
# Leitura Bronze, leitura/escrita Silver+Gold
# =============================================================================

resource "aws_lakeformation_permissions" "engineer_bronze_db" {
  principal   = aws_iam_role.glue_etl.arn
  permissions = ["DESCRIBE"]

  database {
    name = "gpcorp_bronze"
  }
}

resource "aws_lakeformation_permissions" "engineer_bronze_tables" {
  principal   = aws_iam_role.glue_etl.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = "gpcorp_bronze"
    wildcard      = true
  }
}

resource "aws_lakeformation_permissions" "engineer_silver" {
  principal   = aws_iam_role.glue_etl.arn
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.silver.name
  }
}

resource "aws_lakeformation_permissions" "engineer_silver_tables" {
  principal   = aws_iam_role.glue_etl.arn
  permissions = ["ALL"]

  table {
    database_name = aws_glue_catalog_database.silver.name
    wildcard      = true
  }
}

resource "aws_lakeformation_permissions" "engineer_gold" {
  principal   = aws_iam_role.glue_etl.arn
  permissions = ["ALL"]

  database {
    name = aws_glue_catalog_database.gold.name
  }
}

resource "aws_lakeformation_permissions" "engineer_gold_tables" {
  principal   = aws_iam_role.glue_etl.arn
  permissions = ["ALL"]

  table {
    database_name = aws_glue_catalog_database.gold.name
    wildcard      = true
  }
}

# Gold databases por domínio
resource "aws_lakeformation_permissions" "engineer_gold_vendas" {
  principal   = aws_iam_role.glue_etl.arn
  permissions = ["ALL"]

  database {
    name = "gpcorp_gold_vendas"
  }
}

resource "aws_lakeformation_permissions" "engineer_gold_cotacoes" {
  principal   = aws_iam_role.glue_etl.arn
  permissions = ["ALL"]

  database {
    name = "gpcorp_gold_cotacoes"
  }
}

resource "aws_lakeformation_permissions" "engineer_gold_cadastros" {
  principal   = aws_iam_role.glue_etl.arn
  permissions = ["ALL"]

  database {
    name = "gpcorp_gold_cadastros"
  }
}

resource "aws_lakeformation_permissions" "engineer_gold_estoque" {
  principal   = aws_iam_role.glue_etl.arn
  permissions = ["ALL"]

  database {
    name = "gpcorp_gold_estoque"
  }
}

# =============================================================================
# Perfil 3: Analista — somente leitura Gold (QuickSight/Athena)
# =============================================================================

resource "aws_lakeformation_permissions" "analyst_gold_db" {
  principal   = aws_iam_role.analyst.arn
  permissions = ["DESCRIBE"]

  database {
    name = aws_glue_catalog_database.gold.name
  }
}

resource "aws_lakeformation_permissions" "analyst_gold_tables" {
  principal   = aws_iam_role.analyst.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = aws_glue_catalog_database.gold.name
    wildcard      = true
  }
}

# Analista pode ler Gold por domínio
locals {
  gold_databases_analyst = [
    "gpcorp_gold_vendas",
    "gpcorp_gold_cotacoes",
    "gpcorp_gold_cadastros",
    "gpcorp_gold_estoque",
  ]
}

resource "aws_lakeformation_permissions" "analyst_gold_domains_db" {
  for_each    = toset(local.gold_databases_analyst)
  principal   = aws_iam_role.analyst.arn
  permissions = ["DESCRIBE"]

  database {
    name = each.value
  }
}

resource "aws_lakeformation_permissions" "analyst_gold_domains_tables" {
  for_each    = toset(local.gold_databases_analyst)
  principal   = aws_iam_role.analyst.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = each.value
    wildcard      = true
  }
}

# Analista pode ler Silver (PII já mascarado)
resource "aws_lakeformation_permissions" "analyst_silver_db" {
  principal   = aws_iam_role.analyst.arn
  permissions = ["DESCRIBE"]

  database {
    name = aws_glue_catalog_database.silver.name
  }
}

resource "aws_lakeformation_permissions" "analyst_silver_tables" {
  principal   = aws_iam_role.analyst.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = aws_glue_catalog_database.silver.name
    wildcard      = true
  }
}
