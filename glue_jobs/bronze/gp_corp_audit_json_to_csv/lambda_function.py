"""
Lambda - Converte AMOSTRA de JSONs do Bronze (full + incremental) em CSV para Auditoria.
Lê no máximo 2 arquivos JSON por pasta (amostra) para validação rápida.
"""

import json
import csv
import io
import boto3
from datetime import datetime

S3_BUCKET = "gpcorp-datalake"
BRONZE_PREFIX = "Bronze/"
AUDIT_PREFIX = "Audit/"
MAX_FILES_SAMPLE = 2  # Máximo de arquivos JSON a ler por pasta

s3 = boto3.client("s3")


def get_entities_from_bucket():
    result = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=BRONZE_PREFIX, Delimiter="/")
    prefixes = result.get("CommonPrefixes", [])
    entities = [p["Prefix"].replace(BRONZE_PREFIX, "").rstrip("/") for p in prefixes if p["Prefix"].replace(BRONZE_PREFIX, "").rstrip("/")]
    print(f"Entidades encontradas: {entities}")
    return entities


def validate_entity(entity, available_entities):
    if entity not in available_entities:
        raise Exception(f"Entidade '{entity}' não encontrada. Disponíveis: {available_entities}")


def list_s3_objects(prefix):
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                objects.append(obj["Key"])
    return objects


def extract_date_from_partition(key):
    try:
        parts = key.split("/")
        year = month = day = None
        for part in parts:
            if part.startswith("year="):
                year = int(part.split("=")[1])
            elif part.startswith("month="):
                month = int(part.split("=")[1])
            elif part.startswith("day="):
                day = int(part.split("=")[1])
        if year and month and day:
            return datetime(year, month, day).date()
    except (ValueError, TypeError):
        pass
    return None


def filter_files_by_date(json_files, start_date):
    if not start_date:
        return json_files
    cutoff = datetime.strptime(start_date, "%Y-%m-%d").date()
    filtered = [key for key in json_files if (extract_date_from_partition(key) or cutoff) >= cutoff]
    print(f"    Filtro data >= {start_date}: {len(filtered)}/{len(json_files)} arquivos")
    return filtered


def read_json_from_s3(key):
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = response["Body"].read().decode("utf-8")
    try:
        data = json.loads(content)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        return []
    except json.JSONDecodeError:
        pass
    records = []
    for line in content.strip().split("\n"):
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def flatten_record(record):
    flat = {}
    for key, value in record.items():
        if isinstance(value, (list, dict)):
            flat[key] = json.dumps(value, default=str, ensure_ascii=False)
        else:
            flat[key] = value
    return flat


def records_to_csv(records):
    if not records:
        return ""
    flat_records = [flatten_record(r) for r in records]
    all_columns = []
    seen = set()
    for record in flat_records:
        for col in record.keys():
            if col not in seen:
                all_columns.append(col)
                seen.add(col)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(flat_records)
    return output.getvalue()


def save_csv_to_s3(csv_content, s3_key):
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_content.encode("utf-8"), ContentType="text/csv")
    print(f"  ✓ CSV salvo: s3://{S3_BUCKET}/{s3_key}")


def process_full_load(entity):
    prefix = f"{BRONZE_PREFIX}{entity}/full/"
    json_files = list_s3_objects(prefix)

    if not json_files:
        print(f"  {entity}/full: nenhum arquivo encontrado")
        return 0

    # Amostra: pega no máximo 2 arquivos
    sample_files = json_files[:MAX_FILES_SAMPLE]
    print(f"  {entity}/full: {len(json_files)} arquivos total, amostra: {len(sample_files)}")

    total_records = 0
    for i, key in enumerate(sample_files):
        records = read_json_from_s3(key)
        if not records:
            continue

        csv_content = records_to_csv(records)

        if len(sample_files) == 1:
            output_key = f"{AUDIT_PREFIX}{entity}/full/{entity}_full_sample.csv"
        else:
            output_key = f"{AUDIT_PREFIX}{entity}/full/{entity}_full_sample_part{i+1}.csv"

        save_csv_to_s3(csv_content, output_key)
        total_records += len(records)
        del records
        del csv_content

    return total_records


def process_incremental_load(entity, start_date=None):
    prefix = f"{BRONZE_PREFIX}{entity}/incremental/"
    json_files = list_s3_objects(prefix)

    if not json_files:
        print(f"  {entity}/incremental: nenhum arquivo encontrado")
        return 0

    json_files = filter_files_by_date(json_files, start_date)
    if not json_files:
        print(f"  {entity}/incremental: nenhum arquivo após filtro")
        return 0

    # Agrupa por partição
    partitions = {}
    for key in json_files:
        parts = key.split("/")
        partition_parts = [p for p in parts if p.startswith("year=") or p.startswith("month=") or p.startswith("day=")]
        partition_key = "/".join(partition_parts)
        if partition_key not in partitions:
            partitions[partition_key] = []
        partitions[partition_key].append(key)

    # Amostra: pega no máximo 2 partições (dias)
    sorted_partitions = sorted(partitions.items())
    sample_partitions = sorted_partitions[-MAX_FILES_SAMPLE:]  # Últimos 2 dias
    print(f"  {entity}/incremental: {len(sorted_partitions)} dias total, amostra: {len(sample_partitions)} dias")

    total_records = 0
    for partition, files in sample_partitions:
        # Pega no máximo 2 arquivos por partição
        sample_files = files[:MAX_FILES_SAMPLE]
        records = []
        for key in sample_files:
            records.extend(read_json_from_s3(key))

        if not records:
            continue

        csv_content = records_to_csv(records)
        output_key = f"{AUDIT_PREFIX}{entity}/incremental/{partition}/{entity}_incremental_sample.csv"
        save_csv_to_s3(csv_content, output_key)
        total_records += len(records)
        print(f"    {partition}: {len(records)} registros (amostra)")

    return total_records


def lambda_handler(event, context):
    """
    Handler - Gera CSV de amostra (máx 2 arquivos por pasta).

    Evento:
    {
        "full": true,
        "start_date": "2026-06-03",
        "entity": "ALL"
    }
    """
    process_full = event.get("full", True)
    start_date = event.get("start_date")
    entity_filter = event.get("entity", "ALL")

    print(f"=== JSON → CSV Audit (AMOSTRA: máx {MAX_FILES_SAMPLE} arquivos) ===")
    print(f"Full: {process_full} | Start Date: {start_date} | Entity: {entity_filter}")

    available_entities = get_entities_from_bucket()

    if entity_filter == "ALL":
        entities_to_process = available_entities
    else:
        validate_entity(entity_filter, available_entities)
        entities_to_process = [entity_filter]

    results = []

    for entity in entities_to_process:
        print(f"\n{'='*50}")
        print(f"Processando: {entity}")

        full_count = 0
        incremental_count = 0

        try:
            if process_full:
                full_count = process_full_load(entity)

            incremental_count = process_incremental_load(entity, start_date=start_date)

            results.append({
                "entity": entity,
                "full_records_sample": full_count,
                "incremental_records_sample": incremental_count
            })

        except Exception as e:
            print(f"  ERRO em {entity}: {e}")
            results.append({"entity": entity, "error": str(e)})

    print(f"\n{'='*50}")
    print(f"=== CONCLUÍDO ===")
    print(json.dumps(results, indent=2))

    return {"statusCode": 200, "body": json.dumps(results)}
