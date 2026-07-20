"""
Lambda - Quebra JSONs grandes em arquivos menores.
Lê via streaming linha por linha (sem ijson), salva em partes, e deleta o original.
Suporta JSON array e JSONL. Usa apenas libs nativas do Lambda.
"""

import json
import boto3

S3_BUCKET = "gpcorp-datalake"

s3 = boto3.client("s3")


def split_large_json(source_key, records_per_file=10000):
    """
    Lê um JSON grande via streaming (linha por linha), salva em partes menores,
    e deleta o arquivo original.
    """
    print(f"Processando: s3://{S3_BUCKET}/{source_key}")

    base_key = source_key.rsplit(".json", 1)[0]

    response = s3.get_object(Bucket=S3_BUCKET, Key=source_key)
    file_size_mb = response["ContentLength"] / (1024 * 1024)
    print(f"  Tamanho: {file_size_mb:.1f} MB")

    body = response["Body"]

    chunk = []
    part_num = 0
    total_records = 0
    buffer = ""

    # Lê em blocos de 1MB
    for data_chunk in body.iter_lines():
        line = data_chunk.decode("utf-8").strip()

        # Ignora linhas de abertura/fechamento de array
        if line in ("[", "]", ""):
            continue

        # Remove vírgula trailing
        if line.endswith(","):
            line = line[:-1]

        # Tenta parsear como JSON
        try:
            record = json.loads(line)
            chunk.append(record)
        except json.JSONDecodeError:
            # Pode ser um objeto multi-linha, acumula no buffer
            buffer += line
            try:
                record = json.loads(buffer)
                chunk.append(record)
                buffer = ""
            except json.JSONDecodeError:
                continue

        # Salva chunk quando atingir limite
        if len(chunk) >= records_per_file:
            part_num += 1
            _save_chunk(chunk, base_key, part_num)
            total_records += len(chunk)
            chunk = []

    # Salva último chunk
    if chunk:
        part_num += 1
        _save_chunk(chunk, base_key, part_num)
        total_records += len(chunk)

    # Se não conseguiu parsear via streaming, tenta leitura completa
    if total_records == 0:
        print("  Streaming não funcionou, tentando leitura completa em partes...")
        total_records, part_num = _split_full_read(source_key, base_key, records_per_file)

    print(f"  Total: {total_records} registros em {part_num} partes")

    # Deleta o arquivo original
    s3.delete_object(Bucket=S3_BUCKET, Key=source_key)
    print(f"  ✓ Original deletado: {source_key}")

    return {
        "source": source_key,
        "size_mb": round(file_size_mb, 2),
        "total_records": total_records,
        "parts_created": part_num,
        "records_per_file": records_per_file,
        "deleted_original": True
    }


def _split_full_read(source_key, base_key, records_per_file):
    """
    Fallback: lê o JSON inteiro (para arquivos que não são line-delimited).
    Usa para arquivos menores que cabem na memória da Lambda.
    """
    response = s3.get_object(Bucket=S3_BUCKET, Key=source_key)
    content = response["Body"].read().decode("utf-8")

    # Tenta JSON array
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        # Tenta JSONL
        data = []
        for line in content.strip().split("\n"):
            if line.strip():
                try:
                    data.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue

    if not isinstance(data, list):
        data = [data]

    total_records = len(data)
    part_num = 0

    for i in range(0, total_records, records_per_file):
        part_num += 1
        chunk = data[i:i + records_per_file]
        _save_chunk(chunk, base_key, part_num)

    del data  # Libera memória
    return total_records, part_num


def _save_chunk(records, base_key, part_num):
    """Salva um chunk de registros como JSON."""
    part_key = f"{base_key}_part{part_num}.json"
    content = json.dumps(records, default=str, ensure_ascii=False)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=part_key,
        Body=content.encode("utf-8"),
        ContentType="application/json"
    )
    print(f"    Part {part_num}: {len(records)} registros → {part_key}")


def find_large_files(prefix, min_size_mb=100):
    """Encontra arquivos JSON maiores que min_size_mb."""
    large_files = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            size_mb = obj["Size"] / (1024 * 1024)
            if obj["Key"].endswith(".json") and size_mb >= min_size_mb:
                large_files.append({
                    "key": obj["Key"],
                    "size_mb": round(size_mb, 2)
                })

    return large_files


def lambda_handler(event, context):
    """
    Handler.

    Evento:
    {
        "mode": "auto",              // "auto" = encontra e quebra todos > 100MB
                                     // "manual" = quebra arquivo específico
        "source_key": "Bronze/...",  // só para mode=manual
        "records_per_file": 10000,   // registros por parte
        "min_size_mb": 100,          // tamanho mínimo (só para auto)
        "prefix": "Bronze/"          // onde procurar (só para auto)
    }
    """
    mode = event.get("mode", "manual")
    records_per_file = event.get("records_per_file", 10000)
    min_size_mb = event.get("min_size_mb", 100)
    prefix = event.get("prefix", "Bronze/")

    results = []
    mode="manual"
    if mode == "manual":
        #source_key = event.get("source_key")
        source_key = "Bronze/Items/full/year=2026/month=06/day=02/Items_20260602_184835.json"
        if not source_key:
            return {"statusCode": 400, "body": "source_key é obrigatório no modo manual"}

        result = split_large_json(source_key, records_per_file)
        results.append(result)

    elif mode == "auto":
        print(f"Buscando arquivos > {min_size_mb} MB em {prefix}...")
        large_files = find_large_files(prefix, min_size_mb)

        if not large_files:
            print("Nenhum arquivo grande encontrado.")
            return {"statusCode": 200, "body": json.dumps({"message": "Nenhum arquivo grande encontrado"})}

        print(f"Encontrados {len(large_files)} arquivo(s) grande(s):")
        for f in large_files:
            print(f"  {f['key']} ({f['size_mb']} MB)")

        for file_info in large_files:
            try:
                result = split_large_json(file_info["key"], records_per_file)
                results.append(result)
            except Exception as e:
                print(f"  ERRO em {file_info['key']}: {e}")
                results.append({"source": file_info["key"], "error": str(e)})

    print(f"\n=== CONCLUÍDO ===")
    print(json.dumps(results, indent=2))

    return {
        "statusCode": 200,
        "body": json.dumps(results, indent=2)
    }
