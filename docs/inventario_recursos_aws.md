# Inventário de Recursos AWS — GP Corp Datalake
**Conta:** 892748149777 (gpcorpbr)  
**Região:** us-east-1  
**Data:** Julho 2026

---

## Recursos no repositório (Terraform/scripts)

| Recurso | Nome | Arquivo |
|---|---|---|
| Glue Job | gpcorp-silver-dimensions | infra/glue_silver_terraform.tf |
| Glue Job | gpcorp-silver-facts | infra/glue_silver_terraform.tf |
| Glue Job | gpcorp-silver-quality-checks | infra/glue_silver_terraform.tf |
| Glue Job | gpcorp-silver-compaction | cli/create_job_compaction.json |
| Glue Job | gpcorp-gold-dashboards | infra/glue_gold_terraform.tf |
| Glue Job | gpcorp-gold-features | infra/glue_gold_terraform.tf |
| Glue Job | gpcorp-gold-estoque | infra/glue_gold_terraform.tf |
| Glue Job | gpcorp-gold-quality-checks | infra/glue_gold_terraform.tf |
| Glue Job | gpcorp-gold-credit-features | glue_jobs/gold/job_credit_features.py |
| Glue Job | gpcorp-gold-cadastros | cli/create_job_cadastros.json |
| Glue Workflow | gpcorp-silver-pipeline | infra/glue_silver_terraform.tf |
| Glue Crawler | gpcorp-bronze-crawler | infra/glue_bronze_crawler.tf |
| Step Functions | GPCorp_data_lake_pipeline | infra/step_functions_pipeline.json |
| Lambda | gpcorp-extraction-retry | glue_jobs/silver/lambda_retry.py |
| Lambda | gpcorp-collect-metrics | glue_jobs/silver/lambda_metrics.py |
| SQS | gpcorp-extraction-retry | infra/retry_queue_terraform.tf |
| SQS | gpcorp-extraction-dlq | infra/retry_queue_terraform.tf |
| SNS | gpcorp-glue-pipeline-alerts | infra/observability_terraform.tf |
| CloudWatch Alarms (13) | gpcorp-* | infra/observability_terraform.tf |
| IAM Role | GlueServiceRole-gpcorp | infra/iam_roles_terraform.tf |
| IAM Role | AnalystRole-gpcorp | infra/iam_roles_terraform.tf |
| Glue Trigger | gpcorp-silver-compaction-weekly | cli/create_compaction_trigger.json |

---

## Recursos FORA do repositório (criados via console/CLI)

### Lambdas de Extração Bronze

| Nome | Runtime | Origem dos parâmetros |
|---|---|---|
| gp_corp_business_partner_api_ingestion | python3.14 | Secrets Manager: api_gp_corp_datalake_ingestion |
| gp_corp_items_api_ingestion | python3.14 | idem |
| gp_corp_item_groups_api_ingestion | python3.14 | idem |
| gp_corp_sales_persons_api_ingestion | python3.14 | idem |
| gp_corp_invoices_api_ingestion | python3.14 | idem |
| gp_corp_orders_api_ingestion | python3.14 | idem |
| gp_corp_quotations_api_ingestion | python3.14 | idem |
| gp_corp_inventory_gen_entries_api_ingestion | python3.14 | idem |
| gp_corp_audit_json_to_csv | python3.14 | — |
| gp_corp_audit_count_records | python3.14 | — |
| gp_corp_lambda_audit_count_records | python3.14 | — |

**Secrets Manager:** `api_gp_corp_datalake_ingestion` — contém credenciais de acesso ao SAP B1 Service Layer (URL, usuário, senha). **Nunca versionar no Git.**

### Glue Crawler (forecast)

| Nome | Database | S3 Target | Role |
|---|---|---|---|
| gp_corp_forecast | forecast | s3://gpcorp-datalake/Gold/forecast/ | AmazonSageMakerAdminIAMExecutionRole_1 |

> **Nota:** usa a role SageMaker em vez de GlueServiceRole-gpcorp devido a restrição de SCP (AmazonSecurityLakePermissionsBoundary) que bloqueia CloudWatch Logs para a role Glue.

### SageMaker Domains (Unified Studio)

4 domínios ativos — todos criados pelo DataZone/SageMaker Unified Studio. Não gerenciados por Terraform neste repositório.

| Nome do domínio | Status |
|---|---|
| SageMakerUnifiedStudio-cudlhhi9vdbspz-... | InService |
| SageMakerUnifiedStudio-4hf65pd6ibq1pj-... | InService |
| SageMakerUnifiedStudio-bzlgkm8bc5ih7r-... | InService |
| SageMakerUnifiedStudio-bex4yldz31gl5z-... | InService |

### QuickSight Datasources (Athena)

| Nome | Uso |
|---|---|
| Gold_Vendas_vendas_detalhada | Tabela principal vendas |
| Gold_Vendas_faturamento_mensal | KPIs mensais |
| Gold_Vendas_ranking_vendedores | Ranking comercial |
| Gold_Vendas_vendas_por_produto | Análise de mix |
| Gold_Cotacoes_features_predicao_conversao | Features ML |
| Gold_Cotacoes_taxa_conversao (cotacoes_gold) | Pipeline comercial |
| Gold_Cadastros_analise_credito | Crédito |
| Gold_Estoque_movimentacao_estoque | Estoque |
| vendas_gold, cadastros_gold, estoque_gold | Datasources consolidados |
| perfil_cliente, cobertura_vendedor, catalogo_produtos_ativo | Carteira |
| data_quality, qualidade_dados | Painel DQ |
| forecast | Forecast SageMaker |

### S3 Buckets

| Bucket | Uso |
|---|---|
| gpcorp-datalake | Principal: Bronze, Silver, Gold, scripts, métricas, dbt |
| gpcorp-athena-results | Resultados de queries Athena |

### Glue Databases

| Database | Conteúdo | Gerenciado por |
|---|---|---|
| gpcorp_bronze | Tabelas catalogadas pelo crawler Bronze | Terraform |
| gpcorp_silver | Tabelas Silver (Iceberg) | Terraform + Glue Jobs |
| gpcorp_gold_vendas | Tabelas Gold domínio vendas | Glue Jobs |
| gpcorp_gold_cotacoes | Tabelas Gold domínio cotações | Glue Jobs |
| gpcorp_gold_cadastros | Tabelas Gold domínio cadastros | Glue Jobs |
| gpcorp_gold_estoque | Tabelas Gold domínio estoque | Glue Jobs |
| gpcorp_metrics | Métricas de execução dos jobs | Lambda collect-metrics |
| gpcorp_silver_elementary | Histórico dbt/Elementary | dbt run |
| gpcorp_silver_dbt_test_audit | Falhas antigas (schema antigo) | dbt |
| gpcorp_silver_gold_dbt_test_audit | Falhas atuais Silver+Gold | dbt |
| forecast | Tabelas de forecast SageMaker | SageMaker/Glue Crawler |
| sagemaker_sample_db | Dados de exemplo SageMaker | SageMaker |

---

## Como exportar código das Lambdas de extração

Para ter o código local das Lambdas criadas via console:

```powershell
# Para cada Lambda de extração
$lambdas = @(
    "gp_corp_business_partner_api_ingestion",
    "gp_corp_items_api_ingestion",
    "gp_corp_item_groups_api_ingestion",
    "gp_corp_sales_persons_api_ingestion",
    "gp_corp_invoices_api_ingestion",
    "gp_corp_orders_api_ingestion",
    "gp_corp_quotations_api_ingestion",
    "gp_corp_inventory_gen_entries_api_ingestion"
)

foreach ($fn in $lambdas) {
    $url = aws lambda get-function --function-name $fn --query "Code.Location" --output text 2>$null
    Invoke-WebRequest -Uri $url -OutFile "glue_jobs/bronze/$fn.zip"
    Write-Host "Downloaded: $fn"
}
```

---

## Recursos que precisam ser versionados

Para garantir Infrastructure as Code completo, os seguintes recursos devem ser adicionados ao repositório:

| Prioridade | Recurso | Ação |
|---|---|---|
| Alta | Lambdas de extração Bronze (8) | Exportar código + criar Terraform |
| Alta | Secrets Manager (estrutura, sem valores) | Criar resource Terraform sem valores sensíveis |
| Média | Glue Crawler forecast | Adicionar ao Terraform |
| Média | QuickSight datasources | Documentar como código (não há provider Terraform maduro) |
| Baixa | SageMaker Domains | Fora do escopo deste repositório |
