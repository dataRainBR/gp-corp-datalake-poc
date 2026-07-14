# Critérios de Aceite — POC Lakehouse GP Corp BR

**Versão:** 1.0  
**Data:** 21/05/2026  
**Owner:** Fredi / Time de Dados  
**Documento base:** Briefing POC Lakehouse GP Corp BR — Ingestão SAP B1

---

## 1. Critérios de Aceite Obrigatórios (Go/No-Go)

Todos devem ser atendidos para aprovação da POC.

### 1.1 Ingestão e Pipeline

| # | Critério | Condição de Aceite | Como Evidenciar |
|---|----------|-------------------|-----------------|
| CA-01 | Ingestão automatizada | Pipeline de extração SAP B1 → Bronze executando diariamente em janela noturna (00h–05h) sem intervenção manual | Logs CloudWatch mostrando execuções agendadas |
| CA-02 | Estabilidade | ≥ 95% de taxa de sucesso nas últimas 10 execuções consecutivas | Relatório de runs com status e duração |
| CA-03 | SLA de disponibilidade | Dados D-1 disponíveis para consulta até 7h da manhã | Timestamp de conclusão do pipeline vs threshold 07:00 |
| CA-04 | Tempo de carga | Pipeline completo (extração + Bronze + Silver + Gold) < 2h | Duração end-to-end registrada no CloudWatch |
| CA-05 | Idempotência | Reprocessamento do mesmo dia não duplica dados (executar 2x = mesmo resultado) | Rodar pipeline 2x e comparar contagem/totais antes e depois |
| CA-06 | Volume histórico | Capacidade demonstrada de processar ≥ 12 meses de histórico SAP B1 | Tabelas Silver/Gold com dados de pelo menos 12 meses carregados |

### 1.2 Arquitetura e Qualidade de Dados

| # | Critério | Condição de Aceite | Como Evidenciar |
|---|----------|-------------------|-----------------|
| CA-07 | Medallion implementado | 3 camadas operacionais: Bronze (raw, imutável), Silver (limpa, tipada, dedup), Gold (agregada) | Buckets/prefixos S3 + tabelas Glue Catalog em cada camada |
| CA-08 | Formato Iceberg/Delta | Tabelas Silver e Gold em formato aberto (Iceberg) com suporte a MERGE e time-travel | `DESCRIBE TABLE` mostrando formato Iceberg |
| CA-09 | Qualidade Silver | ≥ 99% de registros aprovados em checks de: schema, PK não nula, integridade de FK | Relatório do quality gate (job `gpcorp-silver-quality-checks`) |
| CA-10 | SCD2 nas dimensões | Dimensões críticas (BusinessPartners, Items, SalesPersons) preservam histórico de alterações | Consulta mostrando versões anteriores com `_valid_from`/`_valid_to` |
| CA-11 | Reconciliação com origem | Divergência < 0,1% entre totais de vendas/cotações no SAP vs Lakehouse | Comparativo numérico documentado (totais por período) |
| CA-12 | Catálogo de dados | Todas as tabelas registradas no Glue Data Catalog com descrição e schema | Print do Glue Catalog com tabelas populadas |

### 1.3 Segurança e Governança

| # | Critério | Condição de Aceite | Como Evidenciar |
|---|----------|-------------------|-----------------|
| CA-13 | Controle de acesso RBAC | Mínimo 3 perfis via Lake Formation: admin, engenheiro, analista | Demonstração: analista não acessa Bronze; engenheiro não altera Gold de produção |
| CA-14 | Mascaramento PII (LGPD) | Campos pessoais (CPF, telefone, email) mascarados na Silver via hash SHA-256 | Query na Silver mostrando campos PII com hash, não valor original |
| CA-15 | Criptografia | Dados em repouso (S3 SSE-KMS) e em trânsito (TLS 1.2+) | Configuração do bucket S3 + policy de endpoint |
| CA-16 | Logs de auditoria | CloudTrail + Lake Formation audit logs ativos | Print de logs de acesso a tabelas sensíveis |
| CA-17 | Credenciais seguras | Nenhuma credencial hardcoded; uso de Secrets Manager ou IAM roles | Revisão de código confirmando ausência de secrets |
| CA-18 | Retenção fiscal | Dados de NF (Invoices) retidos por mínimo 5 anos | Lifecycle policy do S3 documentada |

### 1.4 Casos de Uso

| # | Critério | Condição de Aceite | Como Evidenciar |
|---|----------|-------------------|-----------------|
| CA-19 | Dashboards CU-01 | Dashboards de vendas, cotações e performance comercial renderizando dados D-1 no QuickSight | Demonstração ao vivo com dados reais e filtros funcionais |
| CA-20 | Performance queries | P95 < 30s para queries analíticas típicas dos dashboards | Medição de tempo de 10 queries representativas via Athena |
| CA-21 | Feature table CU-02 | Tabela `feature_win_rate` consumível em Python (SageMaker/notebook) com features calculadas | Notebook executando EDA sobre Gold sem fricção |

### 1.5 Monitoramento

| # | Critério | Condição de Aceite | Como Evidenciar |
|---|----------|-------------------|-----------------|
| CA-22 | Alertas de falha | Alarmes CloudWatch disparando notificação (email/SNS) em caso de falha no pipeline | Simulação de falha → alerta recebido |
| CA-23 | Dashboard operacional | Dashboard CloudWatch com métricas: duração de jobs, registros processados, falhas | Print do dashboard com dados reais |

### 1.6 Documentação

| # | Critério | Condição de Aceite | Como Evidenciar |
|---|----------|-------------------|-----------------|
| CA-24 | README do pipeline | Documentação técnica com arquitetura, fluxo de dados e instruções de deploy | Arquivo README.md revisado e completo |
| CA-25 | Dicionário de dados Gold | Descrição de todas as tabelas e colunas da camada Gold | Documento ou catálogo com significado de cada campo |
| CA-26 | Runbook operacional | Procedimentos para: reprocessamento, rollback, investigação de falhas | Documento com passos claros |

---

## 2. Critérios Desejáveis (não bloqueiam aprovação)

| # | Critério | Benefício |
|---|----------|-----------|
| CD-01 | Checks de qualidade com Great Expectations ou dbt tests | Cobertura de qualidade mais granular |
| CD-02 | Lineage visual (OpenLineage / DataHub) | Rastreabilidade completa do dado |
| CD-03 | Dashboard de observabilidade da plataforma | Visão consolidada de saúde do lakehouse |
| CD-04 | Incremental loads (CDC) | Redução de janela de carga e custo |
| CD-05 | Custo mensal documentado | Baseline para projeção de produção |

---

## 3. Metas Quantitativas (resumo)

| Indicador | Meta |
|-----------|------|
| SLA de atualização | Dados D-1 até 7h |
| Tempo de carga e2e | < 2h |
| Taxa de sucesso | ≥ 95% (últimas 10 runs) |
| Reconciliação SAP vs Lakehouse | Divergência < 0,1% |
| Qualidade Silver | ≥ 99% aprovação |
| Performance queries | P95 < 30s |
| Volume | ≥ 12 meses histórico |
| Retenção NF | ≥ 5 anos |

---