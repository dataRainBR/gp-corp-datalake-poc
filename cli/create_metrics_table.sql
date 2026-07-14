-- Criar database e tabela para métricas de execução (rodar no Athena)
CREATE DATABASE IF NOT EXISTS gpcorp_metrics;

CREATE EXTERNAL TABLE IF NOT EXISTS gpcorp_metrics.job_executions (
  data_execucao STRING,
  timestamp_coleta STRING,
  job_name STRING,
  job_short STRING,
  run_id STRING,
  status STRING,
  duracao_segundos INT,
  duracao_minutos DOUBLE,
  workers INT,
  worker_type STRING,
  started_at STRING,
  error_message STRING
)
PARTITIONED BY (year INT, month INT, day INT)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://gpcorp-datalake/metrics/job_executions/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Após criar, rodar para detectar partições:
-- MSCK REPAIR TABLE gpcorp_metrics.job_executions;
