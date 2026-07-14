"""
Lambda - Conta registros ÚNICOS totais por pasta no Bronze.
Lê full + incremental, extrai chaves primárias, deduplica e retorna total único.
"""

import json
import boto3

S3_BUCKET = "gpcorp-datalake"
BRONZE_PREFIX = "Bronze/"

s3 = boto3.client("s3")

# Chave primária de cada entidade (para deduplicação)
PRIMARY_KEYS = {
    "Invoices": "DocEntry",
    "Orders": "DocEntry",
    "Quotations": "DocEntry",
    "PurchaseOrders": "DocEntry",
    "PurchaseInvoices": "DocEntry",
    "BusinessPartners": "CardCode",
    "Items": "ItemCode",
    "ItemGroups": "Number",
    "SalesPersons": "SalesEmployeeCode",
    "InventoryGenEntries": "DocEntry",
}


def list_s3_objects(prefix):
    """Lista todos os objetos .json em um prefixo S3."""
    objects = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                objects.append({"key": obj["Key"], "size": obj["Size"]})

    return objects


def read_json_from_s3(key):
    """Lê um arquivo JSON/JSONL do S3 e retorna lista de registros."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = response["Body"].read().decode("utf-8")

    # Tenta JSON array
    try:
        data = json.loads(content)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        return []
    except json.JSONDecodeError:
        pass

    # JSONL
    records = []
    for line in content.strip().split("\n"):
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def extract_keys_from_file(key, pk_field):
    """
    Lê arquivo e extrai apenas as chaves primárias (sem carregar tudo na memória).
    Retorna set de chaves encontradas.
    """
    records = read_json_from_s3(key)
    keys = set()
    for record in records:
        pk_value = record.get(pk_field)
        if pk_value is not None:
            keys.add(pk_value)
    return keys


def get_entities_from_bucket():
    """Descobre as entidades (pastas) no bucket Bronze."""
    result = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=BRONZE_PREFIX, Delimiter="/")
    prefixes = result.get("CommonPrefixes", [])

    entities = []
    for prefix in prefixes:
        entity_name = prefix["Prefix"].replace(BRONZE_PREFIX, "").rstrip("/")
        if entity_name:
            entities.append(entity_name)

    return entities


def count_entity_unique_records(entity):
    """
    Conta registros ÚNICOS de uma entidade (full + incremental combinados).
    Usa a chave primária para deduplicar.
    """
    pk_field = PRIMARY_KEYS.get(entity, "DocEntry")

    # Coleta todas as chaves de full
    full_files = list_s3_objects(f"{BRONZE_PREFIX}{entity}/full/")
    full_keys = set()
    full_total_raw = 0
    for file_info in full_files:
        keys = extract_keys_from_file(file_info["key"], pk_field)
        full_keys.update(keys)
        full_total_raw += len(keys)

    # Coleta todas as chaves de incremental
    incr_files = list_s3_objects(f"{BRONZE_PREFIX}{entity}/incremental/")
    incr_keys = set()
    incr_total_raw = 0
    for file_info in incr_files:
        keys = extract_keys_from_file(file_info["key"], pk_field)
        incr_keys.update(keys)
        incr_total_raw += len(keys)

    # União = registros únicos (full + incremental deduplicados)
    all_unique_keys = full_keys.union(incr_keys)

    # Novos no incremental (não existiam no full)
    new_in_incremental = incr_keys - full_keys

    # Atualizados (existiam no full E apareceram no incremental)
    updated = incr_keys.intersection(full_keys)

    result = {
        "entity": entity,
        "primary_key": pk_field,
        "full": {
            "files": len(full_files),
            "raw_records": full_total_raw,
            "unique_records": len(full_keys)
        },
        "incremental": {
            "files": len(incr_files),
            "raw_records": incr_total_raw,
            "unique_records": len(incr_keys),
            "new_records": len(new_in_incremental),
            "updated_records": len(updated)
        },
        "total_unique_records": len(all_unique_keys),
        "total_raw_records": full_total_raw + incr_total_raw,
        "duplicates_removed": (full_total_raw + incr_total_raw) - len(all_unique_keys)
    }

    return result


def lambda_handler(event, context):
    """
    Conta registros únicos totais (full + incremental deduplicados).

    Evento:
    {
        "entity": "ALL"       // "ALL" ou nome específico
    }

    Retorna por entidade:
    - raw_records: total bruto (com duplicados)
    - unique_records: total após deduplicar pela chave primária
    - new_records: registros no incremental que NÃO existiam no full
    - updated_records: registros no incremental que JÁ existiam no full
    - duplicates_removed: quantos duplicados foram encontrados
    """
    entity_filter = event.get("entity", "ALL")

    entities = get_entities_from_bucket()

    if entity_filter != "ALL":
        if entity_filter not in entities:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": f"Entidade '{entity_filter}' não encontrada",
                    "available": entities
                })
            }
        entities = [entity_filter]

    print(f"=== CONTAGEM DE REGISTROS ÚNICOS ===")
    print(f"Entidades: {entities}")

    results = []
    grand_total_unique = 0
    grand_total_raw = 0

    for entity in entities:
        print(f"\n{'='*50}")
        print(f"{entity}:")

        try:
            counts = count_entity_unique_records(entity)
            results.append(counts)
            grand_total_unique += counts["total_unique_records"]
            grand_total_raw += counts["total_raw_records"]

            print(f"  Full: {counts['full']['files']} arquivos, {counts['full']['unique_records']} únicos (de {counts['full']['raw_records']} brutos)")
            print(f"  Incremental: {counts['incremental']['files']} arquivos, {counts['incremental']['unique_records']} únicos")
            print(f"    → Novos: {counts['incremental']['new_records']}")
            print(f"    → Atualizados: {counts['incremental']['updated_records']}")
            print(f"  TOTAL ÚNICO: {counts['total_unique_records']} (removidos {counts['duplicates_removed']} duplicados)")

        except Exception as e:
            print(f"  ERRO: {e}")
            results.append({"entity": entity, "error": str(e)})

    print(f"\n{'='*50}")
    print(f"GRAND TOTAL ÚNICO: {grand_total_unique} registros")
    print(f"GRAND TOTAL BRUTO: {grand_total_raw} registros")
    print(f"DUPLICADOS REMOVIDOS: {grand_total_raw - grand_total_unique}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "results": results,
            "grand_total_unique": grand_total_unique,
            "grand_total_raw": grand_total_raw,
            "duplicates_removed": grand_total_raw - grand_total_unique
        }, indent=2)
    }
