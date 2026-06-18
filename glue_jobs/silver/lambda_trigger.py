"""
Lambda: Trigger diário para pipeline Silver.
Calcula D-1 e dispara os jobs com --load_type=incremental e --load_date=YYYY-MM-DD.

Deploy:
  zip lambda_trigger.zip lambda_trigger.py
  aws lambda create-function --function-name gpcorp-silver-trigger \
    --runtime python3.12 --handler lambda_trigger.handler \
    --role arn:aws:iam::892748149777:role/GlueServiceRole-gpcorp \
    --zip-file fileb://lambda_trigger.zip --timeout 30

Schedule (EventBridge):
  aws events put-rule --name gpcorp-silver-daily --schedule-expression "cron(0 2 * * ? *)"
  aws events put-targets --rule gpcorp-silver-daily --targets "Id"="1","Arn"="arn:aws:lambda:us-east-1:892748149777:function:gpcorp-silver-trigger"
"""
import boto3
from datetime import datetime, timedelta

glue = boto3.client("glue")


def handler(event, context):
    # Usa a data de hoje (extração já completou antes deste trigger)
    load_date = datetime.utcnow().strftime("%Y-%m-%d")
    print(f"[TRIGGER] Disparando pipeline Silver para load_date={load_date}")

    # 1. Dimensões (incremental)
    response_dims = glue.start_job_run(
        JobName="gpcorp-silver-dimensions",
        Arguments={
            "--load_type": "incremental",
            "--load_date": load_date,
        }
    )
    print(f"[TRIGGER] Dimensões: {response_dims['JobRunId']}")

    # 2. Fatos (incremental) — roda em paralelo
    response_facts = glue.start_job_run(
        JobName="gpcorp-silver-facts",
        Arguments={
            "--load_type": "incremental",
            "--load_date": load_date,
        }
    )
    print(f"[TRIGGER] Fatos: {response_facts['JobRunId']}")

    return {
        "load_date": load_date,
        "dimensions_run": response_dims["JobRunId"],
        "facts_run": response_facts["JobRunId"],
    }
