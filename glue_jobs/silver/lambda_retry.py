"""
Lambda: Retry Handler para falhas de extração Bronze.
Consome mensagens da SQS retry queue e re-executa a extração.

Fluxo:
  1. Extração falha → mensagem enviada para gpcorp-extraction-retry
  2. SQS trigger esta Lambda
  3. Lambda tenta re-executar a extração
  4. Após 3 falhas → mensagem vai para DLQ → alerta SNS

Formato da mensagem SQS:
{
    "entity": "Invoices",
    "load_date": "2026-06-10",
    "attempt": 1,
    "error": "Connection timeout",
    "source": "sap_service_layer"
}
"""
import json
import os
import boto3
from datetime import datetime

glue = boto3.client("glue")
sqs = boto3.client("sqs")
sns = boto3.client("sns")

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_QUEUE_URL = os.environ.get("RETRY_QUEUE_URL", "")
DLQ_URL = os.environ.get("DLQ_URL", "")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")


def handler(event, context):
    """Processa mensagens de retry da fila SQS."""
    results = []

    for record in event.get("Records", []):
        body = json.loads(record["body"])
        entity = body.get("entity", "unknown")
        load_date = body.get("load_date", datetime.utcnow().strftime("%Y-%m-%d"))
        attempt = body.get("attempt", 1)
        error = body.get("error", "unknown")

        print(f"[RETRY] entity={entity}, load_date={load_date}, attempt={attempt}/{MAX_RETRIES}")

        if attempt >= MAX_RETRIES:
            # Esgotou retries — alerta via SNS
            _send_failure_alert(entity, load_date, attempt, error)
            results.append({
                "entity": entity,
                "status": "FAILED_PERMANENT",
                "attempts": attempt,
            })
            continue

        # Tenta re-executar via Glue job (incremental)
        try:
            # Identifica se é dimensão ou fato
            dimensions = {"BusinessPartners", "Items", "ItemGroups", "SalesPersons"}
            job_name = (
                "gpcorp-silver-dimensions" if entity in dimensions
                else "gpcorp-silver-facts"
            )

            response = glue.start_job_run(
                JobName=job_name,
                Arguments={
                    "--load_type": "incremental",
                    "--load_date": load_date,
                    "--entities": entity,
                }
            )

            print(f"[RETRY] Job {job_name} started: {response['JobRunId']}")
            results.append({
                "entity": entity,
                "status": "RETRIED",
                "job_run_id": response["JobRunId"],
                "attempt": attempt + 1,
            })

        except Exception as e:
            print(f"[ERROR] Retry falhou para {entity}: {e}")

            # Re-enfileira com attempt incrementado
            retry_msg = {
                "entity": entity,
                "load_date": load_date,
                "attempt": attempt + 1,
                "error": str(e),
                "source": body.get("source", "retry_handler"),
            }

            sqs.send_message(
                QueueUrl=RETRY_QUEUE_URL,
                MessageBody=json.dumps(retry_msg),
                # Backoff exponencial: 60s, 120s, 240s
                DelaySeconds=min(60 * (2 ** attempt), 900),
            )

            results.append({
                "entity": entity,
                "status": "REQUEUED",
                "next_attempt": attempt + 1,
            })

    return {"results": results}


def _send_failure_alert(entity: str, load_date: str, attempts: int, error: str):
    """Envia alerta SNS para falha permanente."""
    if not ALERT_TOPIC_ARN:
        print("[WARN] ALERT_TOPIC_ARN não configurado")
        return

    message = (
        f"🚨 Falha permanente na extração\n\n"
        f"Entidade: {entity}\n"
        f"Data: {load_date}\n"
        f"Tentativas: {attempts}\n"
        f"Último erro: {error}\n\n"
        f"Ação necessária: verificar conexão com SAP B1 Service Layer"
    )

    sns.publish(
        TopicArn=ALERT_TOPIC_ARN,
        Subject=f"[GPCORP] Extração falhou: {entity} ({load_date})",
        Message=message,
    )
    print(f"[ALERT] SNS enviado para {entity}")
