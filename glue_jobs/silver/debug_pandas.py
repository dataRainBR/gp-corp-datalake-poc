"""
Debug local das transformações Silver — pandas puro (sem Spark, sem Java, sem Hadoop).
Reproduz exatamente a lógica de utils.py/job_dimensions.py/job_facts.py em pandas.

Uso:
  python glue_jobs/silver/debug_pandas.py --entity BusinessPartners
  python glue_jobs/silver/debug_pandas.py --entity Invoices
  python glue_jobs/silver/debug_pandas.py --all

Requisitos:
  pip install pandas
"""
import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from glob import glob
from pathlib import Path

import pandas as pd

# --- Config ---
SAMPLES_DIR = str(Path(__file__).parent.parent.parent / "data" / "samples")
OUTPUT_DIR = str(Path(__file__).parent.parent.parent / "data" / "output")

ENTITIES = {
    "BusinessPartners": {
        "primary_key": ["CardCode"],
        "pii_columns": ["Phone1", "Phone2", "Cellular", "EmailAddress", "FederalTaxID"],
        "scd_type": 2,
        "select_cols": [
            "CardCode", "CardName", "CardType", "GroupCode", "Phone1", "Phone2",
            "Fax", "Cellular", "EmailAddress", "FederalTaxID", "Address", "ZipCode",
            "City", "County", "Country", "BillToState", "ShipToState",
            "SalesPersonCode", "Currency", "PayTermsGrpCode", "PriceListNum",
            "DiscountPercent", "CreditLimit", "MaxCommitment", "CurrentAccountBalance",
            "OpenDeliveryNotesBalance", "OpenOrdersBalance", "CardForeignName",
            "Valid", "ValidFrom", "ValidTo", "Frozen", "FrozenFrom", "FrozenTo",
            "Block", "CreateDate", "UpdateDate",
        ],
        "date_cols": ["CreateDate", "UpdateDate", "ValidFrom", "ValidTo", "FrozenFrom", "FrozenTo"],
    },
    "Items": {
        "primary_key": ["ItemCode"],
        "pii_columns": [],
        "scd_type": 2,
        "select_cols": [
            "ItemCode", "ItemName", "ForeignName", "ItemsGroupCode", "BarCode",
            "VatLiable", "PurchaseItem", "SalesItem", "InventoryItem", "Mainsupplier",
            "DesiredInventory", "MinInventory", "QuantityOnStock",
            "QuantityOrderedFromVendors", "QuantityOrderedByCustomers",
            "Valid", "ValidFrom", "ValidTo", "Frozen", "FrozenFrom", "FrozenTo",
            "SalesUnit", "PurchaseUnit", "Manufacturer", "CommissionPercent",
            "CommissionGroup", "TreeType", "AssetItem",
        ],
        "date_cols": ["ValidFrom", "ValidTo", "FrozenFrom", "FrozenTo"],
    },
    "ItemGroups": {
        "primary_key": ["Number"],
        "pii_columns": [],
        "scd_type": 1,
        "select_cols": [
            "Number", "GroupName", "ProcurementMethod", "InventorySystem",
            "PlanningSystem", "ItemClass", "Alert", "RawMaterial",
        ],
        "date_cols": [],
    },
    "SalesPersons": {
        "primary_key": ["SalesEmployeeCode"],
        "pii_columns": ["Telephone", "Mobile", "Email"],
        "scd_type": 2,
        "select_cols": [
            "SalesEmployeeCode", "SalesEmployeeName", "Remarks",
            "CommissionForSalesEmployee", "CommissionGroup", "Locked",
            "EmployeeID", "Active", "Telephone", "Mobile", "Fax", "Email",
        ],
        "date_cols": [],
    },
    "Invoices": {
        "primary_key": ["DocEntry", "LineNum"],
        "pii_columns": [],
        "scd_type": 1,
        "has_lines": True,
        "header_cols": [
            "DocEntry", "DocNum", "DocType", "DocDate", "DocDueDate", "TaxDate",
            "CardCode", "CardName", "NumAtCard", "DocTotal", "VatSum",
            "DiscountPercent", "DocCurrency", "DocRate", "SalesPersonCode",
            "DocumentStatus", "Cancelled", "PaymentGroupCode",
            "TransportationCode", "Series", "CreationDate", "UpdateDate",
            "Comments", "Reference1", "BPL_IDAssignedToInvoice", "NumberOfInstallments",
        ],
        "line_cols": [
            "LineNum", "ItemCode", "ItemDescription", "Quantity", "Price",
            "PriceAfterVAT", "LineTotal", "DiscountPercent", "WarehouseCode",
            "SalesPersonCode", "CostingCode", "AccountCode", "ShipDate",
            "Currency", "BarCode", "GrossProfit", "CFOPCode", "BaseEntry",
            "BaseType", "LineStatus", "FreeOfChargeBP", "GrossBuyPrice",
            "ActualDeliveryDate",
        ],
        "date_cols": ["DocDate", "DocDueDate", "TaxDate", "CreationDate", "UpdateDate"],
        "partition_date_col": "DocDate",
    },
    "Orders": {
        "primary_key": ["DocEntry", "LineNum"],
        "pii_columns": [],
        "scd_type": 1,
        "has_lines": True,
        "header_cols": [
            "DocEntry", "DocNum", "DocType", "DocDate", "DocDueDate", "TaxDate",
            "CardCode", "CardName", "NumAtCard", "DocTotal", "VatSum",
            "DiscountPercent", "DocCurrency", "DocRate", "SalesPersonCode",
            "DocumentStatus", "Cancelled", "PaymentGroupCode",
            "TransportationCode", "Series", "CreationDate", "UpdateDate",
            "Comments", "Reference1", "BPL_IDAssignedToInvoice", "NumberOfInstallments",
        ],
        "line_cols": [
            "LineNum", "ItemCode", "ItemDescription", "Quantity", "Price",
            "PriceAfterVAT", "LineTotal", "DiscountPercent", "WarehouseCode",
            "SalesPersonCode", "CostingCode", "AccountCode", "ShipDate",
            "Currency", "BarCode", "GrossProfit", "CFOPCode", "BaseEntry",
            "BaseType", "LineStatus", "FreeOfChargeBP", "GrossBuyPrice",
            "ActualDeliveryDate",
        ],
        "date_cols": ["DocDate", "DocDueDate", "TaxDate", "CreationDate", "UpdateDate"],
        "partition_date_col": "DocDate",
    },
    "Quotations": {
        "primary_key": ["DocEntry", "LineNum"],
        "pii_columns": [],
        "scd_type": 1,
        "has_lines": True,
        "header_cols": [
            "DocEntry", "DocNum", "DocType", "DocDate", "DocDueDate", "TaxDate",
            "CardCode", "CardName", "NumAtCard", "DocTotal", "VatSum",
            "DiscountPercent", "DocCurrency", "SalesPersonCode",
            "DocumentStatus", "Cancelled", "Series", "CreationDate", "UpdateDate",
            "Comments", "BPL_IDAssignedToInvoice", "NumberOfInstallments",
        ],
        "line_cols": [
            "LineNum", "ItemCode", "ItemDescription", "Quantity", "Price",
            "PriceAfterVAT", "LineTotal", "DiscountPercent", "WarehouseCode",
            "Currency", "GrossProfit", "CFOPCode", "BaseEntry",
            "BaseType", "LineStatus", "FreeOfChargeBP", "GrossBuyPrice",
            "ActualDeliveryDate",
        ],
        "date_cols": ["DocDate", "DocDueDate", "TaxDate", "CreationDate", "UpdateDate"],
        "partition_date_col": "DocDate",
    },
    "InventoryGenEntries": {
        "primary_key": ["DocEntry", "LineNum"],
        "pii_columns": [],
        "scd_type": 1,
        "has_lines": True,
        "header_cols": [
            "DocEntry", "DocNum", "DocType", "DocDate", "DocDueDate",
            "DocTotal", "DocCurrency", "Comments", "JournalMemo",
            "CreationDate", "UpdateDate",
        ],
        "line_cols": [
            "LineNum", "ItemCode", "ItemDescription", "Quantity", "Price",
            "LineTotal", "WarehouseCode", "AccountCode", "CostingCode",
            "GrossProfit", "CFOPCode", "BaseEntry", "BaseType", "LineStatus",
            "FreeOfChargeBP", "GrossBuyPrice", "ActualDeliveryDate",
        ],
        "date_cols": ["DocDate", "DocDueDate", "CreationDate", "UpdateDate"],
        "partition_date_col": "DocDate",
    },
}


def sha256(value) -> str:
    """Hash SHA-256 de um valor."""
    if pd.isna(value) or value is None or value == "":
        return None
    return hashlib.sha256(str(value).encode()).hexdigest()


def bp(step: str, df: pd.DataFrame, show_rows: int = 5):
    """Breakpoint — imprime estado do DataFrame."""
    print(f"\n{'─'*60}")
    print(f"  ▸ [{step}] {len(df)} registros | {len(df.columns)} colunas")
    print(f"{'─'*60}")
    print(f"  Colunas: {list(df.columns)}")
    print(df.head(show_rows).to_string(max_colwidth=40))
    return df


# ═══════════════════════════════════════════════════════════════
# BP1: read_bronze_json
# ═══════════════════════════════════════════════════════════════
def read_bronze_json(entity_name: str) -> pd.DataFrame:
    """Lê JSONs da amostra local."""
    path = os.path.join(SAMPLES_DIR, entity_name)
    json_files = glob(os.path.join(path, "**", "*.json"), recursive=True)
    if not json_files:
        json_files = glob(os.path.join(path, "*.json"))
    if not json_files:
        raise FileNotFoundError(f"Nenhum JSON em: {path}")

    print(f"  Lendo {len(json_files)} arquivo(s) de: {path}")

    all_records = []
    for jf in json_files:
        with open(jf, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for rec in data:
                rec["_source_file"] = os.path.basename(jf)
            all_records.extend(data)
        elif isinstance(data, dict):
            data["_source_file"] = os.path.basename(jf)
            all_records.append(data)

    df = pd.DataFrame(all_records)
    return df


# ═══════════════════════════════════════════════════════════════
# BP2: select/cast (projeção)
# ═══════════════════════════════════════════════════════════════
def select_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Projeta colunas — equivale ao .select() do Spark."""
    available = [c for c in cols if c in df.columns]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"  ⚠ Colunas não encontradas (ignoradas): {missing}")
    result = df[available + ["_source_file"]].copy()
    return result


# ═══════════════════════════════════════════════════════════════
# BP3: clean_strings (trim)
# ═══════════════════════════════════════════════════════════════
def clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Trim e remove quebras de linha em colunas string."""
    str_cols = df.select_dtypes(include=["object"]).columns
    for c in str_cols:
        df[c] = df[c].apply(lambda x: x.replace('\n', ' ').replace('\r', ' ').strip() if isinstance(x, str) else x)
    return df


# ═══════════════════════════════════════════════════════════════
# BP4: validate_not_null_keys
# ═══════════════════════════════════════════════════════════════
def validate_not_null_keys(df: pd.DataFrame, primary_key: list) -> pd.DataFrame:
    """Remove registros com PK nula ou negativa (sentinela SAP: -1 = não atribuído)."""
    antes = len(df)
    df = df.dropna(subset=primary_key)
    # Remove PKs negativas (SAP usa -1 como placeholder "sem vínculo")
    for pk in primary_key:
        if df[pk].dtype in ("int64", "float64", "int32", "float32"):
            negatives = (df[pk] < 0).sum()
            if negatives > 0:
                df = df[df[pk] >= 0]
                print(f"  ⚠ {negatives} registros com {pk} < 0 removidos (sentinela SAP)")
    depois = len(df)
    removidos = antes - depois
    if removidos > 0:
        pct = (depois / antes) * 100 if antes > 0 else 0
        print(f"  ⚠ Total removidos: {removidos}. Taxa válida: {pct:.2f}%")
    return df


# ═══════════════════════════════════════════════════════════════
# BP5: deduplicate
# ═══════════════════════════════════════════════════════════════
def deduplicate(df: pd.DataFrame, primary_key: list) -> pd.DataFrame:
    """Dedup mantendo último por _source_file (desc)."""
    antes = len(df)
    df = df.sort_values("_source_file", ascending=False)
    df = df.drop_duplicates(subset=primary_key, keep="first")
    depois = len(df)
    print(f"  Dedup: {antes} → {depois} (removidos: {antes - depois})")
    return df


# ═══════════════════════════════════════════════════════════════
# BP6: mask_pii
# ═══════════════════════════════════════════════════════════════
def mask_pii(df: pd.DataFrame, pii_columns: list) -> pd.DataFrame:
    """Mascara PII com SHA-256."""
    for c in pii_columns:
        if c in df.columns:
            df[c] = df[c].apply(sha256)
    if pii_columns:
        print(f"  PII mascarado: {[c for c in pii_columns if c in df.columns]}")
    return df


# ═══════════════════════════════════════════════════════════════
# BP7: add_silver_metadata
# ═══════════════════════════════════════════════════════════════
def add_silver_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona colunas de controle."""
    now = datetime.utcnow()
    df["_silver_loaded_at"] = now
    df["_silver_load_date"] = now.date()
    return df


# ═══════════════════════════════════════════════════════════════
# BP8: compute_row_hash (SCD2)
# ═══════════════════════════════════════════════════════════════
def compute_row_hash(df: pd.DataFrame) -> pd.DataFrame:
    """Hash de todas as colunas de negócio para SCD2."""
    exclude = {"_source_file", "_silver_loaded_at", "_silver_load_date", "_sk",
               "_row_hash", "_valid_from", "_valid_to", "_is_current", "odata.etag"}
    hash_cols = sorted([c for c in df.columns if c not in exclude])
    print(f"  Hash SCD2 com {len(hash_cols)} colunas: {hash_cols[:10]}...")

    def row_hash(row):
        vals = "||".join(str(row[c]) if pd.notna(row[c]) else "__NULL__" for c in hash_cols)
        return hashlib.sha256(vals.encode()).hexdigest()

    df["_row_hash"] = df.apply(row_hash, axis=1)
    return df


# ═══════════════════════════════════════════════════════════════
# BP9: generate_surrogate_key
# ═══════════════════════════════════════════════════════════════
def generate_surrogate_key(df: pd.DataFrame, primary_key: list) -> pd.DataFrame:
    """Gera SK baseada na PK."""
    def sk(row):
        vals = "||".join(str(row[c]) for c in primary_key)
        return hashlib.sha256(vals.encode()).hexdigest()
    df["_sk"] = df.apply(sk, axis=1)
    return df


# ═══════════════════════════════════════════════════════════════
# Explode DocumentLines (fatos)
# ═══════════════════════════════════════════════════════════════
def explode_lines(df: pd.DataFrame, header_cols: list, line_cols: list) -> pd.DataFrame:
    """Desnormaliza DocumentLines — equivale ao explode() do Spark."""
    rows = []
    for _, record in df.iterrows():
        lines = record.get("DocumentLines", [])
        if not isinstance(lines, list):
            continue
        for line in lines:
            row = {}
            for hc in header_cols:
                row[hc] = record.get(hc)
            for lc in line_cols:
                # Renomeia campos com conflito de nome header vs line
                if lc == "DiscountPercent":
                    row["LineDiscountPercent"] = line.get(lc)
                elif lc == "SalesPersonCode":
                    row["LineSalesPersonCode"] = line.get(lc)
                elif lc == "CostingCode":
                    row["CostCenter"] = line.get(lc)
                elif lc == "Price":
                    row["UnitPrice"] = line.get(lc)
                else:
                    row[lc] = line.get(lc)
            row["_source_file"] = record.get("_source_file")
            rows.append(row)

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════
# MAIN: Debug de dimensão
# ═══════════════════════════════════════════════════════════════
def debug_dimension(entity_name: str):
    cfg = ENTITIES[entity_name]
    print(f"\n{'='*60}")
    print(f"  DEBUG: {entity_name} (dimensão, SCD{cfg['scd_type']})")
    print(f"{'='*60}")

    # BP1
    print("\n[BP1] read_bronze_json")
    df = read_bronze_json(entity_name)
    df = bp("BP1 - Leitura bruta", df)

    # BP2
    print("\n[BP2] select/cast")
    df = select_columns(df, cfg["select_cols"])
    df = bp("BP2 - Projeção", df)

    # BP3
    print("\n[BP3] clean_strings")
    df = clean_strings(df)
    df = bp("BP3 - Trim", df, 3)

    # BP4
    print("\n[BP4] validate_not_null_keys")
    df = validate_not_null_keys(df, cfg["primary_key"])

    # BP5
    print("\n[BP5] deduplicate")
    df = deduplicate(df, cfg["primary_key"])

    # BP6
    print("\n[BP6] mask_pii")
    df = mask_pii(df, cfg["pii_columns"])
    if cfg["pii_columns"]:
        pii_show = [c for c in cfg["pii_columns"] if c in df.columns]
        print(df[cfg["primary_key"] + pii_show].head(3).to_string())

    # BP7
    print("\n[BP7] add_silver_metadata")
    df = add_silver_metadata(df)

    # BP8
    if cfg["scd_type"] == 2:
        print("\n[BP8] compute_row_hash (SCD2)")
        df = compute_row_hash(df)
        print(df[cfg["primary_key"] + ["_row_hash"]].head(3).to_string())

    # BP9
    print("\n[BP9] generate_surrogate_key")
    df = generate_surrogate_key(df, cfg["primary_key"])

    # Remove auxiliar
    df = df.drop(columns=["_source_file"], errors="ignore")

    # Tipagem de datas
    for dc in cfg.get("date_cols", []):
        if dc in df.columns:
            df[dc] = pd.to_datetime(df[dc], errors="coerce").dt.date

    df = bp("FINAL", df, 5)
    save_output(df, entity_name)
    return df


# ═══════════════════════════════════════════════════════════════
# MAIN: Debug de fato
# ═══════════════════════════════════════════════════════════════
def debug_fact(entity_name: str):
    cfg = ENTITIES[entity_name]
    print(f"\n{'='*60}")
    print(f"  DEBUG: {entity_name} (fato, explode DocumentLines)")
    print(f"{'='*60}")

    # BP1
    print("\n[BP1] read_bronze_json")
    df = read_bronze_json(entity_name)
    df = bp("BP1 - Leitura bruta (headers)", df, 3)

    # BP2: Verifica DocumentLines
    print("\n[BP2] Verificando DocumentLines")
    if "DocumentLines" not in df.columns:
        print("  ✗ ERRO: 'DocumentLines' não encontrado!")
        print(f"  Colunas: {list(df.columns)}")
        return None

    sample_lines = df.iloc[0]["DocumentLines"]
    if isinstance(sample_lines, list):
        print(f"  DocumentLines é lista. Primeiro doc tem {len(sample_lines)} linhas")
        print(f"  Campos da linha: {list(sample_lines[0].keys()) if sample_lines else 'vazio'}")
    else:
        print(f"  ⚠ DocumentLines tipo inesperado: {type(sample_lines)}")

    # BP3: Explode
    print("\n[BP3] explode DocumentLines")
    antes = len(df)
    df = explode_lines(df, cfg["header_cols"], cfg["line_cols"])
    print(f"  Explode: {antes} headers → {len(df)} linhas")
    df = bp("BP3 - Após explode", df, 5)

    # BP4: Clean
    print("\n[BP4] clean_strings")
    df = clean_strings(df)

    # BP5: Validate PK
    print("\n[BP5] validate_not_null_keys")
    df = validate_not_null_keys(df, cfg["primary_key"])

    # BP6: Dedup
    print("\n[BP6] deduplicate")
    df = deduplicate(df, cfg["primary_key"])

    # BP7: Date partitions
    print("\n[BP7] add_date_partitions")
    date_col = cfg.get("partition_date_col", "DocDate")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["year"] = df[date_col].dt.year
    df["month"] = df[date_col].dt.month
    df["day"] = df[date_col].dt.day
    print(f"  Distribuição:")
    print(df.groupby(["year", "month", "day"]).size().to_string())
    print(df.groupby(["year", "month"]).size().to_string())

    # BP8: Metadata
    print("\n[BP8] add_silver_metadata")
    df = add_silver_metadata(df)
    df = df.drop(columns=["_source_file"], errors="ignore")

    # Tipagem datas
    for dc in cfg.get("date_cols", []):
        if dc in df.columns:
            df[dc] = pd.to_datetime(df[dc], errors="coerce").dt.date

    df = bp("FINAL", df, 5)
    save_output(df, entity_name)
    return df


def save_output(df: pd.DataFrame, entity_name: str):
    """Salva resultado como CSV e Parquet."""
    out_dir = os.path.join(OUTPUT_DIR, entity_name)
    os.makedirs(out_dir, exist_ok=True)

    csv_path = os.path.join(out_dir, "result.csv")
    df.to_csv(csv_path, index=False)
    print(f"\n  ✓ CSV salvo: {csv_path}")

    try:
        parquet_path = os.path.join(out_dir, "result.parquet")
        df.to_parquet(parquet_path, index=False)
        print(f"  ✓ Parquet salvo: {parquet_path}")
    except ImportError:
        print("  ⚠ pyarrow não instalado — Parquet não salvo (pip install pyarrow)")


def main():
    parser = argparse.ArgumentParser(description="Debug Silver ETL — pandas puro")
    parser.add_argument("--entity", default="Quotations")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.all:
        entities = list(ENTITIES.keys())
    else:
        entities = [args.entity]

    for entity in entities:
        if entity not in ENTITIES:
            print(f"Entidade '{entity}' não encontrada. Disponíveis: {list(ENTITIES.keys())}")
            sys.exit(1)

        cfg = ENTITIES[entity]
        if cfg.get("has_lines"):
            debug_fact(entity)
        else:
            debug_dimension(entity)

    print(f"\n{'='*60}")
    print(f"  DONE — Outputs em: {OUTPUT_DIR}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
