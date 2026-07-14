"""
Lambda: Coleta métricas de execução dos Glue Jobs e grava em S3 (Athena-readable).
Roda como último passo do Step Functions — após Gold Quality.

Grava: s3://gpcorp-datalake/metrics/job_executions/
Formato: JSON (Athena pode ler direto com SerDe)

Tabela resultante consultável via:
  SELECT * FROM gpcorp_metrics.job_executions ORDER BY data_execucao DESC
"""
import json
import os
import boto3
from datetime import datetime, timezone

glue = boto3.client("glue")
s3 = boto3.client("s3")

BUCKET = "gpcorp-datalake"
PREFIX = "metrics/job_executions"

JOBS = [
    "gpcorp-silver-dimensions",
    "gpcorp-silver-facts",
    "gpcorp-silver-quality-checks",
    "gpcorp-gold-dashboards",
    "gpcorp-gold-features",
    "gpcorp-gold-estoque",
    "gpcorp-gold-cadastros",
    "gpcorp-gold-credit-features",
    "gpcorp-gold-quality-checks",
]


def handler(event, context):
    """Coleta último run de cada job e grava em S3."""
    now = datetime.now(timezone.utc)
    data_execucao = now.strftime("%Y-%m-%d")
    records = []

    for job_name in JOBS:
        try:
            response = glue.get_job_runs(JobName=job_name, MaxResults=1)
            runs = response.get("JobRuns", [])
            if not runs:
                continue

            run = runs[0]
            record = {
                "data_execucao": data_execucao,
                "timestamp_coleta": now.isoformat(),
                "job_name": job_name,
                "job_short": job_name.replace("gpcorp-", "").replace("-", "_"),
                "run_id": run.get("Id", ""),
                "status": run.get("JobRunState", "UNKNOWN"),
                "duracao_segundos": run.get("ExecutionTime", 0),
                "duracao_minutos": round(run.get("ExecutionTime", 0) / 60.0, 1),
                "workers": run.get("NumberOfWorkers", 0),
                "worker_type": run.get("WorkerType", ""),
                "started_at": run.get("StartedOn", "").isoformat() if run.get("StartedOn") else "",
                "error_message": (run.get("ErrorMessage") or "")[:200],
            }
            records.append(record)
            print(f"  {job_name}: {record['status']} ({record['duracao_minutos']} min)")

        except Exception as e:
            print(f"  {job_name}: ERRO ao coletar - {e}")
            records.append({
                "data_execucao": data_execucao,
                "timestamp_coleta": now.isoformat(),
                "job_name": job_name,
                "job_short": job_name.replace("gpcorp-", "").replace("-", "_"),
                "run_id": "",
                "status": "COLLECTION_ERROR",
                "duracao_segundos": 0,
                "duracao_minutos": 0,
                "workers": 0,
                "worker_type": "",
                "started_at": "",
                "error_message": str(e)[:200],
            })

    # Grava como JSON Lines em S3 (particionado por data)
    key = f"{PREFIX}/year={now.year}/month={now.month:02d}/day={now.day:02d}/metrics_{now.strftime('%Y%m%d_%H%M%S')}.json"
    body = "\n".join([json.dumps(r) for r in records])

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )

    print(f"\n[METRICS] {len(records)} registros gravados em s3://{BUCKET}/{key}")

    # Atualiza partições no Athena automaticamente
    athena = boto3.client("athena")
    partition_sql = f"ALTER TABLE gpcorp_metrics.job_executions ADD IF NOT EXISTS PARTITION (year={now.year}, month={now.month}, day={now.day}) LOCATION 's3://{BUCKET}/{PREFIX}/year={now.year}/month={now.month:02d}/day={now.day:02d}/'"
    try:
        athena.start_query_execution(
            QueryString=partition_sql,
            WorkGroup="gpcorp-analytics",
            ResultConfiguration={"OutputLocation": f"s3://{BUCKET}/athena-results/"}
        )
        print(f"[METRICS] Partição {now.year}/{now.month:02d}/{now.day:02d} registrada no Athena")
    except Exception as e:
        print(f"[WARN] Falha ao registrar partição: {e}")

    return {
        "statusCode": 200,
        "records_written": len(records),
        "s3_path": f"s3://{BUCKET}/{key}",
    }
