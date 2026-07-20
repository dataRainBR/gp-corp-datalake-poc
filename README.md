# Documentação Técnica — Lakehouse GP Corp BR
**Versão:** 1.0  
**Data:** Julho 2026  
**Responsável técnico:** Datarain — Time de Dados  
**Projeto:** POC Lakehouse GP Corp BR — Ingestão SAP B1 para AWS

---

## 1. Contexto e motivação

A GP Corp opera com o SAP Business One como sistema de gestão (ERP). Relatórios comerciais e análises de desempenho eram gerados diretamente via queries no banco HANA, o que criava dependência do banco de produção, limitações de performance e dificuldade para cruzar dados históricos.

O objetivo desta POC foi demonstrar a viabilidade técnica de extrair dados do SAP B1 via Service Layer (API REST), armazená-los em AWS com estrutura Medallion (Bronze / Silver / Gold), e disponibilizá-los para dashboards analíticos no Amazon QuickSight — sem impacto no ambiente de produção SAP.

As análises cobertas são: desempenho comercial (faturamento, ranking de vendedores, mix de produtos), pipeline de cotações e taxa de conversão, perfil de clientes (segmentação RFV), e estoque.

---

## 2. Arquitetura geral

```
SAP B1 Service Layer (API)
        │
        ▼
┌─────────────────────────────────────────────────────┐
│              AWS — Conta gpcorpbr (us-east-1)       │
│                                                     │
│  EventBridge ──► Step Functions (orquestrador)      │
│       │                    │                        │
│       │          ┌─────────▼─────────┐              │
│       │          │  Lambdas (x8)     │              │
│       │          │  Extração Bronze  │              │
│       │          └─────────┬─────────┘              │
│       │                    │                        │
│       │          ┌─────────▼─────────┐              │
│       │          │   S3 Bronze       │              │
│       │          │  (JSON raw)       │              │
│       │          └─────────┬─────────┘              │
│       │                    │                        │
│       │          ┌─────────▼─────────┐              │
│       │          │  Glue Jobs Silver │              │
│       │          │  (Spark + Iceberg)│              │
│       │          └─────────┬─────────┘              │
│       │                    │                        │
│       │          ┌─────────▼─────────┐              │
│       │          │   S3 Silver       │              │
│       │          │  (Iceberg tables) │              │
│       │          └─────────┬─────────┘              │
│       │                    │                        │
│       │          ┌─────────▼─────────┐              │
│       │          │  Glue Jobs Gold   │              │
│       │          │  (Spark + Iceberg)│              │
│       │          └─────────┬─────────┘              │
│       │                    │                        │
│       │          ┌─────────▼─────────┐              │
│       │          │   S3 Gold         │              │
│       │          │  (Iceberg tables) │              │
│       │          └─────────┬─────────┘              │
│       │                    │                        │
│       │          ┌─────────▼─────────┐              │
│       │          │ Amazon QuickSight │              │
│       │          │ (via Athena)      │              │
│       │          └───────────────────┘              │
│                                                     │
│  Observabilidade: CloudWatch + SNS + dbt + Lake     │
│  Formation + SQS (retry) + Lambda (métricas)        │
└─────────────────────────────────────────────────────┘
```

**Fluxo diário** (início 00:00 UTC = 21:00 BRT):
1. EventBridge aciona o Step Functions
2. Step Functions executa 8 Lambdas em paralelo (extração Bronze)
3. Glue Crawler atualiza catálogo da Bronze
4. Silver Dimensions (SCD2: clientes, produtos, vendedores, grupos)
5. Silver Facts (incremental: invoices, orders, quotations, inventory)
6. Quality Checks — dbt via Athena (2 minutos)
7. Gold Dashboards (tabelas de vendas/cotações)
8. Gold Features, Estoque, Cadastros, Credit (em paralelo)
9. Gold Quality — dbt via Athena
10. Lambda coleta métricas de execução → Athena

---

## 3. Estrutura de dados (Medallion)

### 3.1 Bronze — dados brutos

Localização: `s3://gpcorp-datalake/Bronze/{Entidade}/{full|incremental}/year=/month=/day=/`

Formato: JSON arrays conforme retornado pelo SAP B1 Service Layer. Dados imutáveis — nunca alterados após gravação.

| Entidade SAP | Tabela HANA | Tipo | Vol. full load |
|---|---|---|---|
| BusinessPartners | OCRD | Dimensão | ~350 MB |
| Items | OITM | Dimensão | ~2,2 GB |
| ItemGroups | OITB | Dimensão | ~128 KB |
| SalesPersons | OSLP | Dimensão | ~41 KB |
| Invoices | OINV + INV1 | Fato | ~8 GB |
| Orders | ORDR + RDR1 | Fato | ~10 GB |
| Quotations | OQUT + QUT1 | Fato | ~294 MB |
| InventoryGenEntries | OIGN | Fato | ~1,5 MB |

### 3.2 Silver — dados transformados

Localização: `s3://gpcorp-datalake/Silver/warehouse/gpcorp_silver.db/{tabela}/`  
Formato: Apache Iceberg (Parquet + ZSTD), catalogado no AWS Glue Data Catalog.

Transformações aplicadas:
- Tipagem de campos (datas, decimais, inteiros)
- Limpeza de strings (trim, remoção de quebras de linha)
- Mascaramento LGPD: campos `Phone1`, `Phone2`, `Cellular`, `FederalTaxID` substituídos por hash SHA-256 quando preenchidos
- Remoção de registros com chave primária nula ou negativa (sentinela SAP = -1)
- Deduplicação por PK (mantém versão mais recente via `_source_file` desc)
- SCD2 nas dimensões: `_valid_from`, `_valid_to`, `_is_current`, `_row_hash` para detecção de mudanças

**Tabelas Silver:**

| Tabela | PK | SCD | Partição |
|---|---|---|---|
| business_partners | CardCode | Tipo 2 | — |
| items | ItemCode | Tipo 2 | — |
| item_groups | Number | Tipo 1 | — |
| sales_persons | SalesEmployeeCode | Tipo 2 | — |
| invoices | DocEntry + LineNum | Tipo 1 (MERGE) | year/month/day |
| orders | DocEntry + LineNum | Tipo 1 (MERGE) | year/month/day |
| quotations | DocEntry + LineNum | Tipo 1 (MERGE) | year/month/day |
| inventory_gen_entries | DocEntry + LineNum | Tipo 1 (MERGE) | year/month/day |

### 3.3 Gold — dados agregados

Localização: `s3://gpcorp-datalake/Gold/gpcorp_gold_{domínio}.db/{tabela}/`  
Formato: Apache Iceberg, recriado integralmente a cada execução (DROP + CREATE).

**Domínio Vendas** (`gpcorp_gold_vendas`):
- `vendas_detalhada` — grão linha de NF, desnormalizado com vendedor, grupo e marca
- `faturamento_mensal` — agregado por mês, vendedor, cliente, produto e filial
- `ranking_vendedores` — métricas mensais por vendedor com posição por receita
- `vendas_por_produto` — vendas por item com classificação ABC

**Domínio Cotações** (`gpcorp_gold_cotacoes`):
- `taxa_conversao` — volume de cotações vs pedidos por vendedor/item/mês
- `features_predicao_conversao` — features ML para predição de win-rate por cliente/vendedor

**Domínio Cadastros** (`gpcorp_gold_cadastros`):
- `perfil_cliente` — segmentação RFV (recência, frequência, valor) e classificação ABC
- `catalogo_produtos_ativo` — catálogo com status de atividade (ativo/lento/encalhado/obsoleto) e marca
- `cobertura_vendedor` — carteira por vendedor com % de clientes ativos
- `analise_credito` — extração de features de crédito via NLP do campo FreeText (LGPD)

**Domínio Estoque** (`gpcorp_gold_estoque`):
- `movimentacao_estoque` — entradas e saídas por item, depósito e mês

---

## 4. Orquestração — Step Functions

O pipeline é disparado pelo EventBridge diariamente às 00:00 UTC (21:00 BRT), dando tempo suficiente para que os dados estejam disponíveis até as 07:00 BRT (SLA contratual).

O Step Functions (`GPCorp_data_lake_pipeline`) orquestra a sequência completa, com tratamento de falha em cada etapa. Em caso de erro nas extrações Bronze, uma mensagem é enviada para a fila SQS de retry (`gpcorp-extraction-retry`) e uma notificação por e-mail é disparada via SNS.

**Configuração de retry para falhas de extração:**
- Tentativas automáticas: 3 (com backoff exponencial de 60s, 120s, 240s)
- Após 3 falhas: mensagem vai para a Dead Letter Queue (`gpcorp-extraction-dlq`) e o time recebe alerta para intervenção manual

---

## 5. Decisões técnicas

### Apache Iceberg

A escolha pelo Iceberg (em vez de Delta Lake ou Hudi) foi motivada por:
- Suporte nativo no AWS Glue 4.0 sem configuração adicional
- MERGE INTO nativo via Spark SQL — necessário para upsert incremental
- Time-travel: possibilidade de consultar versões históricas dos dados
- Sem vendor lock-in (formato aberto, legível por Athena, Spark e outros engines)

### SCD Tipo 2 nas dimensões

Clientes, produtos e vendedores mudam ao longo do tempo (nome, classificação, dados de contato). O SCD2 garante que histórico de vendas seja analisado com os dados vigentes no momento da transação, não com os dados atuais. A detecção de mudança é feita via hash SHA-256 de todos os campos de negócio — evita reprocessamento desnecessário quando não há alteração real.

### Qualidade via dbt + Athena

A primeira implementação usava jobs Spark para quality checks. Com fatos de 8-10 GB e FK checks via left anti-join, os jobs chegavam a 50+ minutos e terminavam em TIMEOUT. A migração para dbt com Athena reduziu o tempo para 2 minutos com custo estimado de R$ 0,08 por execução — contra R$ 5,50 no modelo Spark.

### Compaction semanal

Cada carga incremental diária cria novos arquivos Parquet pequenos (~2-5 MB). Após semanas de operação, tabelas de fatos acumulam centenas de small files, degradando a performance de leitura. Um job de compaction semanal (toda segunda-feira às 01:00 UTC) consolida os arquivos para ~128 MB, mantendo a performance estável.

### Gold: rebuild completo vs MERGE

As tabelas Gold são recriadas do zero a cada execução. A decisão foi intencional: como são derivadas de Silver (que já é idempotente), o rebuild garante consistência sem a complexidade de gerenciar deltas nas agregações. O tempo total do Gold (todos os domínios) é de aproximadamente 25 minutos em paralelo.

---

## 6. Segurança e governança

### IAM Roles

| Role | Uso | Permissões |
|---|---|---|
| `GlueServiceRole-gpcorp` | Glue Jobs, Lambda, Step Functions | S3 R/W Silver+Gold, R Bronze; Glue Catalog CRUD; Lake Formation; Athena |
| `aws-quicksight-service-role-v0` | QuickSight | Athena queries; S3 leitura Gold+Silver |
| `AnalystRole-gpcorp` | Analistas externos | Athena queries; S3 leitura Gold+Silver (sem Bronze) |

### Lake Formation

O Lake Formation controla o acesso no nível de database e tabela. A configuração atual:
- **Admin:** `GlueServiceRole-gpcorp` tem acesso total a todos os databases
- **Analista:** `AnalystRole-gpcorp` tem SELECT em Gold e Silver — não enxerga Bronze
- **IAM_ALLOWED_PRINCIPALS:** habilitado nos databases Gold e Silver para compatibilidade com QuickSight

### Mascaramento LGPD

Campos PII dos BusinessPartners são mascarados na camada Silver com SHA-256 antes de qualquer persistência:
- `Phone1`, `Phone2`, `Cellular` — mascarados quando o cliente tem FederalTaxID preenchido (indica PF)
- `FederalTaxID` — sempre mascarado quando preenchido

O campo `EmailAddress` não é mascarado pois trata-se de e-mail corporativo PJ.

### Retenção fiscal

Dados de Notas Fiscais (Bronze/Invoices) devem ser retidos por 5 anos conforme legislação. **Pendente:** configurar S3 Lifecycle Policy para transição ao Glacier após 12 meses e exclusão após 5 anos.

### Criptografia do bucket

O bucket `gpcorp-datalake` está hoje com criptografia padrão SSE-S3 (AES-256). **Pendente:** migrar para SSE-KMS com CMK dedicada, para atender exigência de criptografia gerenciada por chave própria (rotação, auditoria via CloudTrail, revogação de acesso). Essa mudança não é apenas de configuração do bucket — exige atualizar a key policy da CMK com todas as roles que hoje leem/escrevem no bucket (`GlueServiceRole-gpcorp`, roles das Lambdas de ingestão Bronze, `AnalystRole-gpcorp`, `aws-quicksight-service-role-v0`, role do crawler `gp_corp_forecast`), senão os jobs e Lambdas passam a falhar com AccessDenied no próximo write. Ficou de fora desta rodada por esse impacto operacional; entrar em janela de manutenção planejada antes da homologação.

---

## 7. Monitoramento

### Alarmes CloudWatch

Há 13 alarmes configurados, todos apontando para o SNS topic `gpcorp-glue-pipeline-alerts` com notificação por e-mail para `naiara.fiamoncini@datarain.com.br`:

| Alarme | Condição |
|---|---|
| gpcorp-pipeline-sfn-failed | Step Functions com execução com falha |
| gpcorp-pipeline-not-executed | Pipeline não iniciou em mais de 26h |
| gpcorp-silver-dimensions-failure | Job Silver Dimensions falhou |
| gpcorp-silver-facts-failure | Job Silver Facts falhou |
| gpcorp-gold-dashboards-failure | Job Gold Dashboards falhou |
| gpcorp-gold-features-failure | Job Gold Features falhou |
| gpcorp-gold-estoque-failure | Job Gold Estoque falhou |
| gpcorp-gold-cadastros-failure | Job Gold Cadastros falhou |
| gpcorp-silver-compaction-failure | Job de compaction semanal falhou |
| gpcorp-extraction-dlq-not-empty | Extrações com falha permanente na DLQ |
| gpcorp-lambda-silver-trigger-errors | Lambda de trigger com erros |
| gpcorp-pipeline-duration-sla | Pipeline ultrapassou 2h de execução |

### Dashboard operacional

O CloudWatch Dashboard `gpcorp-lakehouse-pipeline` exibe duração dos jobs, registros processados e tarefas com falha para as últimas 24h. Acesso via Console AWS → CloudWatch → Dashboards.

---

## 8. Qualidade de dados

Os testes de qualidade são executados automaticamente após cada carga Silver e Gold, usando dbt com adaptador Athena. Todos os testes rodam com `severity: warn` (não bloqueiam o pipeline) e `store_failures: true` (persistem registros problemáticos para auditoria).

**Cobertura Silver (28 testes SQL customizados):**
- Unicidade de chaves primárias (invoices, orders, quotations, inventory)
- Consistência SCD2 (apenas um registro corrente por entidade, hash não nulo)
- Integridade referencial: invoices/orders → business_partners (severity: warn, aceita órfãos de ingestão defasada)
- Valores de negócio: quantidades positivas, datas não futuras
- Vendedor: aceita SalesPersonCode = -1 como válido (sentinela SAP B1 para "sem vendedor atribuído" — e-commerce, bonificação)
- Reconciliação fiscal: DocTotal ≈ SUM(LineTotal) + VatSum + DocumentAdditionalExpenses (frete no cabeçalho), tolerância 5%, exclui NFs com dados inconsistentes no SAP (SUM(LineTotal) > DocTotal × 5)

**Cobertura Gold (14 testes SQL customizados):**
- PKs das tabelas de fatos e dimensões
- Ranges de valores (margem, taxa de conversão, scores de crédito)
- Partições válidas
- Reconciliação vendas_detalhada vs faturamento_mensal

**Monitoramento via Elementary:**
- Histórico de execuções em `gpcorp_silver_elementary.elementary_test_results`
- Freshness monitorado via alarme CloudWatch (`gpcorp-pipeline-not-executed`), não via dbt (evita falsos positivos em reprocessamentos parciais)
- Relatório HTML gerado automaticamente em `s3://gpcorp-datalake/dbt/reports/elementary_report.html`

**Taxa de sucesso atual: 97,6%** (41/42 testes pass+warn na última execução). Único error remanescente é freshness de dimensões não reprocessadas neste ciclo — resolve automaticamente na próxima execução completa do pipeline.

**Query para dashboard Data Quality (QuickSight):**
```sql
-- Taxa de Sucesso (última execução)
SELECT
  COUNT(*) as total_testes,
  SUM(CASE WHEN status IN ('pass', 'warn') THEN 1 ELSE 0 END) as testes_ok,
  SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as testes_error,
  ROUND(100.0 * SUM(CASE WHEN status IN ('pass', 'warn') THEN 1 ELSE 0 END)
    / COUNT(*), 1) as taxa_sucesso_pct
FROM gpcorp_silver_elementary.elementary_test_results
WHERE created_at = (
  SELECT MAX(created_at) FROM gpcorp_silver_elementary.elementary_test_results
)
```

**Query Divergência SAP vs Lakehouse:**
```sql
SELECT
  COUNT(DISTINCT docentry) as total_documentos,
  SUM(CASE WHEN pct_divergencia > 5 THEN 1 ELSE 0 END) as docs_divergentes,
  ROUND(100.0 * SUM(CASE WHEN pct_divergencia <= 5 THEN 1 ELSE 0 END)
    / COUNT(*), 2) as pct_reconciliados
FROM (
  SELECT docentry, doctotal,
    CASE WHEN doctotal > 0
      THEN ABS(doctotal - (SUM(linetotal) + MAX(vatsum)
        + MAX(documentadditionalexpenses))) / doctotal * 100
      ELSE 0 END as pct_divergencia
  FROM gpcorp_silver.invoices
  WHERE cancelled != 'tYES'
  GROUP BY docentry, doctotal
) WHERE doctotal > 0
```

Testes com falha persistem os registros problemáticos no database `gpcorp_silver_gold_dbt_test_audit` para auditoria.

---

## 9. Operação

### Execução manual dos jobs

Quando necessário reprocessar fora do ciclo automático, a ordem de execução é:

```
1. gpcorp-silver-dimensions  (--load_type=all ou incremental)
2. gpcorp-silver-facts        (--load_type=all ou incremental)
3. gpcorp-silver-quality-checks
4. gpcorp-gold-dashboards
5. gpcorp-gold-features    ┐
6. gpcorp-gold-estoque     ├─ podem rodar em paralelo
7. gpcorp-gold-credit-features │
8. gpcorp-gold-cadastros   ┘
9. gpcorp-gold-quality-checks
```

Via AWS CLI:
```bash
# Reprocessamento completo Silver
aws glue start-job-run --job-name gpcorp-silver-dimensions \
  --arguments '{"--load_type":"all"}'

aws glue start-job-run --job-name gpcorp-silver-facts \
  --arguments '{"--load_type":"all"}'

# Carga incremental (pipeline diário normal)
aws glue start-job-run --job-name gpcorp-silver-dimensions \
  --arguments '{"--load_type":"incremental","--load_date":"2026-07-13"}'
```

### Reprocessamento de uma entidade específica

```bash
aws glue start-job-run --job-name gpcorp-silver-dimensions \
  --arguments '{"--load_type":"incremental","--load_date":"2026-07-13","--entities":"BusinessPartners"}'
```

### Verificar status do pipeline

```bash
# Últimas 3 execuções do Step Functions
aws glue get-workflow-runs --name gpcorp-silver-pipeline \
  --max-results 3 \
  --query "Runs[].{Status:Status,Started:StartedOn}"

# Status de um job específico
aws glue get-job-runs --job-name gpcorp-gold-dashboards \
  --max-results 1 \
  --query "JobRuns[0].{State:JobRunState,Duration:ExecutionTime}"
```

### Rollback (time-travel Iceberg)

Para restaurar uma tabela Silver para um estado anterior:

```sql
-- Ver snapshots disponíveis
SELECT snapshot_id, committed_at, operation
FROM gpcorp_silver.invoices$snapshots
ORDER BY committed_at DESC;

-- Restaurar para snapshot anterior
CALL glue_catalog.system.rollback_to_snapshot(
  'gpcorp_silver.invoices',
  <snapshot_id>
);
```

### Investigação de falhas

1. **Verificar erro no Glue job:**
```bash
aws logs filter-log-events \
  --log-group-name "/aws-glue/jobs/output" \
  --log-stream-names "<JobRunId>" \
  --filter-pattern "ERROR"
```

2. **Verificar registros problemáticos na qualidade:**
```sql
-- Ver registros que falharam no teste de PK duplicada
SELECT * FROM gpcorp_silver_gold_dbt_test_audit.unique_invoices_pk LIMIT 20;

-- Histórico de execuções dos testes
SELECT test_name, status, failures, created_at
FROM gpcorp_silver_elementary.elementary_test_results
WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
ORDER BY created_at DESC;
```

3. **Verificar divergência de dados:**
```sql
SELECT 'invoices' AS entidade,
       COUNT(DISTINCT docentry) AS total_docs,
       ROUND(SUM(linetotal), 2) AS receita_linhas,
       ROUND(ABS(SUM(DISTINCT doctotal) - SUM(linetotal))
         / NULLIF(SUM(DISTINCT doctotal), 0) * 100, 4) AS pct_divergencia
FROM gpcorp_silver.invoices
WHERE cancelled != 'tYES';
```

---

## 10. Estrutura do repositório

```
gp-corp-datalake-poc/
├── glue_jobs/
│   ├── bronze/
│   │   ├── ingestion/              # 8 Lambdas de extração SAP B1 → S3
│   │   │   ├── gp_corp_invoices_api_ingestion/
│   │   │   ├── gp_corp_orders_api_ingestion/
│   │   │   ├── gp_corp_quotations_api_ingestion/
│   │   │   ├── gp_corp_business_partner_api_ingestion/
│   │   │   ├── gp_corp_items_api_ingestion/
│   │   │   ├── gp_corp_item_groups_api_ingestion/
│   │   │   ├── gp_corp_sales_persons_api_ingestion/
│   │   │   └── gp_corp_inventory_gen_entries_api_ingestion/
│   │   └── utils/                  # Lambdas utilitárias
│   │       ├── gp_corp_audit_json_to_csv/
│   │       ├── gp_corp_audit_count_records/
│   │       ├── gp_corp_lambda_audit_count_records/
│   │       ├── lambda_split_large_json/
│   │       └── gpcorp-extraction-retry/
│   ├── silver/                     # Glue Jobs Bronze → Silver (PySpark + Iceberg)
│   │   ├── job_dimensions.py       # SCD2: BP, Items, SalesPersons, ItemGroups
│   │   ├── job_facts.py            # MERGE: Invoices, Orders, Quotations, Inventory
│   │   ├── iceberg_writer.py       # Escrita Iceberg com schema evolution
│   │   ├── scd2.py                 # Lógica SCD Tipo 2
│   │   ├── utils.py                # Helpers (leitura Bronze, dedup, metadata)
│   │   ├── config.py               # Configuração centralizada de entidades
│   │   ├── compaction.py           # Compaction semanal (rewrite_data_files)
│   │   ├── lambda_metrics.py       # Lambda coleta métricas de execução
│   │   └── debug_pandas.py         # Debug local sem Spark
│   └── gold/                       # Glue Jobs Silver → Gold (PySpark + Iceberg)
│       ├── job_dashboards.py       # vendas_detalhada, faturamento, ranking, produto
│       ├── job_features_predicao_conversao.py
│       ├── job_cadastros.py        # perfil_cliente, catalogo, cobertura_vendedor
│       ├── job_estoque.py          # movimentacao_estoque
│       ├── job_credit_features.py  # analise_credito (NLP FreeText)
│       └── config.py               # Databases Gold + paths
├── dbt/
│   ├── run_dbt_tests.py            # Glue Python Shell: executa dbt tests
│   └── gpcorp_quality/             # Projeto dbt (quality checks)
│       ├── models/                 # sources.yml (Silver + Gold)
│       ├── tests/silver/           # 28 testes SQL customizados
│       ├── tests/gold/             # 14 testes SQL customizados
│       ├── dbt_project.yml
│       ├── packages.yml            # dbt_utils + elementary
│       └── profiles.yml            # Athena adapter config
├── infra/                          # Terraform (IaC)
│   ├── iam_roles_terraform.tf      # 3 roles: Glue, Lambda, Analyst
│   ├── lake_formation_terraform.tf # RBAC (grants por database/tabela)
│   ├── glue_silver_terraform.tf    # Jobs Silver + Workflow
│   ├── glue_gold_terraform.tf      # Jobs Gold
│   ├── glue_bronze_crawler.tf      # Crawler Bronze
│   ├── step_functions_terraform.tf # Step Functions orquestrador
│   ├── step_functions_pipeline.json # ASL definition
│   ├── lambda_terraform.tf         # Lambda trigger
│   ├── retry_queue_terraform.tf    # SQS retry + DLQ
│   └── observability_terraform.tf  # 13 alarmes + SNS + Dashboard
├── cli/                            # Scripts operacionais (PowerShell + JSON)
│   ├── deploy_infra.ps1            # Deploy Terraform
│   ├── setup_rbac_safe.ps1         # Aplica grants Lake Formation
│   ├── check_perms.ps1             # Valida permissões RBAC
│   └── *.json                      # Políticas IAM, triggers, grants
├── docs/                           # Documentação
│   ├── documentacao_tecnica.md     # Doc principal (este README é cópia)
│   ├── dicionario_dados_gold.md    # Schema de todas as tabelas Gold
│   ├── arquitetura_quicksight_dashboard.md
│   ├── inventario_recursos_aws.md
│   └── ...
├── sagemaker/                      # Módulo ML/Forecast (SageMaker)
│   ├── eda/                        # Análise exploratória
│   ├── processing/                 # Preparação de dados (dataset_builder, preprocessing)
│   ├── training/                   # Treinamento e avaliação do modelo
│   ├── inference/                  # Predição (predict.py)
│   ├── pipelines/                  # Pipelines SageMaker (full, prediction)
│   ├── utils/                      # Utilitários (logs)
│   ├── scripts/                    # Scripts de entrada (CLI)
│   └── notebooks/                  # Notebooks Jupyter (EDA, pipelines, testes)
├── data/
│   └── output/                     # Resultados de testes locais (gitignore)
├── requirements.txt                # Dependências Python
└── .gitignore
```

---


## 11. Pendências para produção

As seguintes atividades estão mapeadas como necessárias antes da homologação em produção:

- **S3 Lifecycle Policy** para retenção fiscal de NFs (5 anos) — ver seção 6, "Retenção fiscal"
- **Criptografia SSE-KMS** no bucket S3 — ver seção 6, "Criptografia do bucket" (requer coordenar atualização de permissões de todas as roles antes de trocar o algoritmo padrão)
- **Separação de roles IAM** — produção deve ter roles distintas por ambiente
- **Dashboards QuickSight** — criação no ambiente de produção
- **Validação de reconciliação** com totais extraídos diretamente do SAP B1
- **Runbook operacional** detalhado para equipe da GP Corp
- **Modelo operacional** — definição de responsabilidades, SLAs e procedimentos de escalada
