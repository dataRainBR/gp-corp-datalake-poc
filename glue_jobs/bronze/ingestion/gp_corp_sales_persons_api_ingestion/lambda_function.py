"""
Lambda Unificada - Carga FULL e INCREMENTAL do SAP Business One → S3
Controla via parâmetros no evento qual entidade extrair e se é full ou incremental.
Suporta auto-invocação recursiva para volumes grandes (> 15 min).
"""

import json
import time
import urllib3
import boto3
import os
from datetime import datetime, timedelta, timezone

# Desabilita warning SSL (SAP pode usar certificado self-signed)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager(cert_reqs="CERT_NONE")


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def get_secret(secret_id: str, secret_key: str) -> str:
    """Obtém um segredo do AWS Secrets Manager"""
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name="us-east-1")
    response = client.get_secret_value(SecretId=secret_id)
    secret = json.loads(response["SecretString"])
    return secret[secret_key]


# ============================================================
# FUNÇÕES SAP SERVICE LAYER
# ============================================================

def sap_login():
    """Faz login na Service Layer e retorna o SessionId."""
    url = f"https://{API_ENDPOINT}/b1s/v1/Login"
    payload = json.dumps({
        "CompanyDB": API_COMPANY,
        "UserName": API_USER,
        "Password": API_PASSWORD
    })

    response = http.request("POST", url, body=payload,
                            headers={"Content-Type": "application/json"})

    if response.status == 200:
        data = json.loads(response.data.decode("utf-8"))
        return data["SessionId"]
    else:
        raise Exception(f"Login falhou: {response.status} - {response.data.decode('utf-8')}")


def sap_logout(session_id):
    """Encerra sessão na Service Layer."""
    url = f"https://{API_ENDPOINT}/b1s/v1/Logout"
    http.request("POST", url, headers={"Cookie": f"B1SESSION={session_id}"})


def sap_get_entity_table(session_id, entity, top=100, skip=0, filters=None, select=None):
    """Faz GET em uma entidade da Service Layer."""
    url = f"https://{API_ENDPOINT}/b1s/v1/{entity}"

    params = [f"$top={top}", f"$skip={skip}"]
    if filters:
        params.append(f"$filter={filters}")
    if select:
        params.append(f"$select={select}")
    url += "?" + "&".join(params)

    response = http.request("GET", url, headers={
        "Content-Type": "application/json",
        "Cookie": f"B1SESSION={session_id}",
        "Prefer": "odata.maxpagesize=100"
    })

    if response.status == 200:
        return json.loads(response.data.decode("utf-8"))
    else:
        raise Exception(f"GET {entity} falhou: {response.status} - {response.data.decode('utf-8')}")


def sap_get_all_pages(session_id, entity, filters=None, select=None, page_size=100):
    """Busca TODOS os registros com paginação automática."""
    all_records = []
    skip = 0

    while True:
        result = sap_get_entity_table(session_id, entity, top=page_size, skip=skip,
                                      filters=filters, select=select)
        records = result.get("value", [])
        all_records.extend(records)

        print(f"  {entity} - Página {skip // page_size + 1}: {len(records)} registros (total: {len(all_records)})")

        if len(records) < page_size:
            break

        skip += page_size

    return all_records


def sap_get_all_pages_with_checkpoint(session_id, entity, filters=None, select=None,
                                       page_size=100, start_skip=0, max_seconds=300):
    """
    Busca registros com paginação. Se estiver perto do timeout (13 min),
    salva o que tem e retorna o skip atual para continuar na próxima invocação.

    Args:
        max_seconds: tempo máximo em segundos antes de parar (default: 780 = 13 min | 300 = 5 min)
        start_skip: skip inicial (para continuar de onde parou)

    Returns:
        (records, next_skip) - next_skip é None se terminou tudo
    """
    all_records = []
    skip = start_skip
    start_time = time.time()

    while True:
        # Verifica se está perto do timeout
        elapsed = time.time() - start_time
        if elapsed > max_seconds:
            print(f"  ⚠️ Perto do timeout ({elapsed:.0f}s). Checkpoint: skip={skip}")
            return all_records, skip

        result = sap_get_entity_table(session_id, entity, top=page_size, skip=skip,
                                      filters=filters, select=select)
        records = result.get("value", [])
        all_records.extend(records)

        print(f"  {entity} - skip={skip}: {len(records)} registros (total: {len(all_records)})")

        if len(records) < page_size:
            return all_records, None  # None = terminou tudo

        skip += page_size

    return all_records, None


# ============================================================
# SALVAR NO S3
# ============================================================

def save_to_s3(data, entity, load_type="full"):
    """Salva dados no S3 em formato JSON, particionado por data."""
    if not data:
        print(f"  {entity}: nenhum dado para salvar")
        return

    s3 = boto3.client("s3")
    now = datetime.utcnow()

    # S3_PREFIX permite adicionar caminho base (ex: "Bronze/")
    s3_key = (
        f"{S3_PREFIX}"
        f"{entity}/{load_type}/"
        f"year={now.strftime('%Y')}/"
        f"month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/"
        f"{entity}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )

    json_content = json.dumps(data, default=str, ensure_ascii=False)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json_content.encode("utf-8"),
        ContentType="application/json"
    )
    print(f"  ✓ {entity}: {len(data)} registros → s3://{S3_BUCKET}/{s3_key}")


# ============================================================
# CONFIGURAÇÃO DAS ENTIDADES
# ============================================================

ENTITIES_FULL_LOAD = {
    # Vendas
    "Invoices": {"filter_field": "DocDate", "has_date_filter": True},
    "Orders": {"filter_field": "DocDate", "has_date_filter": True},
    "Quotations": {"filter_field": "DocDate", "has_date_filter": True},
    # Cadastros (sem filtro de data)
    "BusinessPartners": {"filter_field": None, "has_date_filter": False},
    "Items": {"filter_field": None, "has_date_filter": False},
    "ItemGroups": {"filter_field": None, "has_date_filter": False},
    "SalesPersons": {"filter_field": None, "has_date_filter": False},
    # Estoque
    "InventoryGenEntries": {"filter_field": "DocDate", "has_date_filter": True},
}

ENTITIES_INCREMENTAL_LOAD = {
    # Vendas (UpdateDate pega criações E alterações)
    "Invoices": {"filter_field": "UpdateDate"},
    "Orders": {"filter_field": "UpdateDate"},
    "Quotations": {"filter_field": "UpdateDate"},
    # Cadastros
    "BusinessPartners": {"filter_field": "UpdateDate"},
    "Items": {"filter_field": "UpdateDate"},
    # Cadastros pequenos (sem UpdateDate → full reload sempre)
    "ItemGroups": {"filter_field": None, "full_always": True},
    "SalesPersons": {"filter_field": None, "full_always": True},
    # Estoque
    "InventoryGenEntries": {"filter_field": "DocDate"},
}


# ============================================================
# FUNÇÕES DE CARGA
# ============================================================

def get_entity_config(entity: str, incremental: bool) -> dict:
    """Retorna configuração da entidade baseado no tipo de carga."""
    if incremental:
        config = ENTITIES_INCREMENTAL_LOAD.get(entity)
    else:
        config = ENTITIES_FULL_LOAD.get(entity)

    if not config:
        raise Exception(f"Entidade '{entity}' não configurada para {'incremental' if incremental else 'full'} load")

    return config


def sap_data_full_load(session_id, entity, config, start_date="2024-01-01",
                       start_skip=0, context=None, event=None):
    """
    Carga TOTAL de uma entidade desde start_date.
    Suporta checkpoint e auto-invocação para volumes grandes.
    """
    print(f"\n=== FULL LOAD: {entity} (desde {start_date}, skip={start_skip}) ===")

    # Monta filtro
    if config["has_date_filter"]:
        filters = f"{config['filter_field']} ge '{start_date}'"
    else:
        filters = None

    # Busca com checkpoint (para antes do timeout)
    records, next_skip = sap_get_all_pages_with_checkpoint(
        session_id, entity, filters=filters, start_skip=start_skip
    )

    # Salva os registros desta execução no S3
    save_to_s3(records, entity, load_type="full")

    # Se não terminou, re-invoca a Lambda para continuar
    if next_skip is not None and context:
        print(f"  🔄 Re-invocando Lambda para continuar: skip={next_skip}")
        lambda_client = boto3.client("lambda")

        # Monta evento para próxima invocação
        next_event = {
            "entity": entity,
            "incremental": False,
            "start_date": start_date,
            "start_skip": next_skip,
            "current_invocation": (event or {}).get("current_invocation", 1) + 1,
            "max_invocations": (event or {}).get("max_invocations", 20)
        }

        lambda_client.invoke(
            FunctionName=context.function_name,
            InvocationType="Event",  # Assíncrono - não espera resposta
            Payload=json.dumps(next_event)
        )

        return {
            "entity": entity,
            "records": len(records),
            "load_type": "full",
            "status": "partial",
            "next_skip": next_skip
        }

    print(f"  FULL LOAD {entity}: {len(records)} registros extraídos (completo)")
    return {"entity": entity, "records": len(records), "load_type": "full", "status": "complete"}


def sap_data_incremental_load(session_id, entity, config, target_date=None,
                              start_skip=0, context=None, event=None):
    """
    Carga INCREMENTAL de uma entidade (registros novos/alterados).
    Usa target_date = ONTEM por padrão para garantir que não perde registros
    por diferença de fuso horário (SAP grava em horário local, Lambda usa UTC).
    
    Para tabelas sem UpdateDate (full_always=True), faz full reload completo.
    """
    # Tabelas pequenas sem UpdateDate → full reload sempre
    if config.get("full_always"):
        print(f"\n=== FULL RELOAD (sem UpdateDate): {entity} ===")
        records = sap_get_all_pages(session_id, entity)
        save_to_s3(records, entity, load_type="incremental")
        print(f"  FULL RELOAD {entity}: {len(records)} registros extraídos")
        return {"entity": entity, "records": len(records), "load_type": "full_reload", "status": "complete"}

    # Tabelas com UpdateDate → incremental com janela ontem+hoje
    if not target_date:
        # Usa ONTEM como padrão → pega tudo de ontem + hoje (janela de 2 dias)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        target_date = yesterday

    print(f"\n=== INCREMENTAL LOAD: {entity} (desde {target_date}, skip={start_skip}) ===")

    # Filtro
    filter_field = config["filter_field"]
    filters = f"{filter_field} ge '{target_date}'"

    # Busca com checkpoint
    records, next_skip = sap_get_all_pages_with_checkpoint(
        session_id, entity, filters=filters, start_skip=start_skip
    )

    # Salva no S3
    save_to_s3(records, entity, load_type="incremental")

    # Se não terminou, re-invoca
    if next_skip is not None and context:
        print(f"  🔄 Re-invocando Lambda para continuar: skip={next_skip}")
        lambda_client = boto3.client("lambda")

        next_event = {
            "entity": entity,
            "incremental": True,
            "target_date": target_date,
            "start_skip": next_skip,
            "current_invocation": (event or {}).get("current_invocation", 1) + 1,
            "max_invocations": (event or {}).get("max_invocations", 20)
        }

        lambda_client.invoke(
            FunctionName=context.function_name,
            InvocationType="Event",
            Payload=json.dumps(next_event)
        )

        return {
            "entity": entity,
            "records": len(records),
            "load_type": "incremental",
            "status": "partial",
            "next_skip": next_skip
        }

    print(f"  INCREMENTAL {entity}: {len(records)} registros extraídos (completo)")
    return {"entity": entity, "records": len(records), "load_type": "incremental", "status": "complete"}


# ============================================================
# VARIÁVEIS DE AMBIENTE
# ============================================================

SECRET_ID = os.environ["SECRET_ID"]
S3_PATH = os.environ["S3_PATH"]

# Separa bucket e prefixo do S3_PATH
# Aceita: "s3://bucket/prefix/", "bucket/prefix", "bucket"
_s3_path = S3_PATH.replace("s3://", "").rstrip("/")
_parts = _s3_path.split("/", 1)
S3_BUCKET = _parts[0]                         # "gpcorp-datalake"
S3_PREFIX = _parts[1] + "/" if len(_parts) > 1 else ""  # "Bronze/"

API_ENDPOINT = get_secret(secret_id=SECRET_ID, secret_key="API_ENDPOINT")
API_USER = get_secret(secret_id=SECRET_ID, secret_key="API_USER")
API_COMPANY = get_secret(secret_id=SECRET_ID, secret_key="API_COMPANY")
API_PASSWORD = get_secret(secret_id=SECRET_ID, secret_key="API_PASSWORD")


# ============================================================
# HANDLER
# ============================================================

def lambda_handler(event, context):
    """
    Handler unificado com suporte a auto-invocação recursiva.

    Evento:
    {
        "entity": "Invoices",          // qual entidade (ou "ALL")
        "incremental": true,           // true = incremental, false = full
        "start_date": "2024-01-01",    // só para full load
        "target_date": "2026-06-02",   // só para incremental (default: hoje)
        "start_skip": 0,              // para continuar de onde parou (auto-preenchido)
        "current_invocation": 1,       // contador de invocações (auto-preenchido)
        "max_invocations": 20          // limite de segurança contra loop infinito
    }
    """
    # Parâmetros do evento
    entity = event.get("entity", "SalesPersons")
    incremental = event.get("incremental", True)
    start_date = event.get("start_date", "2024-01-01")
    target_date = event.get("target_date")  # None = usará ontem como padrão na função
    start_skip = event.get("start_skip", 0)
    current_invocation = event.get("current_invocation", 1)
    max_invocations = event.get("max_invocations", 20)

    # Proteção contra loop infinito
    if current_invocation > max_invocations:
        msg = f"⛔ Limite de {max_invocations} invocações atingido. Parando."
        print(msg)
        return {"statusCode": 200, "body": json.dumps({"message": msg})}

    print(f"Evento: entity={entity}, incremental={incremental}, skip={start_skip}, invocação={current_invocation}/{max_invocations}")

    # Define quais entidades processar
    if entity == "ALL":
        if incremental:
            entities_to_process = list(ENTITIES_INCREMENTAL_LOAD.keys())
        else:
            entities_to_process = list(ENTITIES_FULL_LOAD.keys())
    else:
        entities_to_process = [entity]

    session_id = None
    results = []

    try:
        # Login
        session_id = sap_login()
        print("Login OK")

        # Processa cada entidade
        for ent in entities_to_process:
            try:
                config = get_entity_config(ent, incremental)

                if incremental:
                    result = sap_data_incremental_load(
                        session_id, ent, config, target_date,
                        start_skip=start_skip, context=context, event=event
                    )
                else:
                    result = sap_data_full_load(
                        session_id, ent, config, start_date,
                        start_skip=start_skip, context=context, event=event
                    )

                results.append(result)

                # Se re-invocou (partial), para de processar outras entidades nesta execução
                if result.get("status") == "partial":
                    break

            except Exception as e:
                print(f"  ERRO em {ent}: {e}")
                results.append({"entity": ent, "error": str(e)})

    except Exception as e:
        print(f"ERRO CRÍTICO: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    finally:
        if session_id:
            sap_logout(session_id)
            print("\nLogout OK")

    print(f"\n=== CONCLUÍDO (invocação {current_invocation}) ===")
    print(json.dumps(results, indent=2))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "load_type": "incremental" if incremental else "full",
            "invocation": current_invocation,
            "results": results
        })
    }
