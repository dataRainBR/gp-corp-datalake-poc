"""
Debug local das transformações Silver — sem custos AWS.
Usa PySpark standalone com amostras da Bronze em disco.

Uso:
  pip install pyspark==3.4.1
  python debug_local.py --entity BusinessPartners

Requisitos:
  - Amostras em data/samples/{Entity}/*.json
  - Não precisa de Iceberg/Glue Catalog (escrita desabilitada)
"""
import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, row_number, desc, sha2, concat_ws,
    coalesce, lit, when, trim, to_date, current_timestamp, year, month,
    explode
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType


# --- Config local (espelha config.py sem dependências AWS) ---

SAMPLES_DIR = str(Path(__file__).parent.parent.parent / "data" / "samples")

ENTITIES_LOCAL = {
    "BusinessPartners": {
        "primary_key": ["CardCode"],
        "pii_columns": ["Phone1", "Phone2", "Cellular", "EmailAddress", "FederalTaxID"],
        "scd_type": 2,
    },
    "Items": {
        "primary_key": ["ItemCode"],
        "pii_columns": [],
        "scd_type": 2,
    },
    "ItemGroups": {
        "primary_key": ["Number"],
        "pii_columns": [],
        "scd_type": 1,
    },
    "SalesPersons": {
        "primary_key": ["SalesEmployeeCode"],
        "pii_columns": ["Telephone", "Mobile", "Email"],
        "scd_type": 2,
    },
    "Invoices": {
        "primary_key": ["DocEntry", "LineNum"],
        "pii_columns": [],
        "scd_type": 1,
        "has_lines": True,
    },
    "Orders": {
        "primary_key": ["DocEntry", "LineNum"],
        "pii_columns": [],
        "scd_type": 1,
        "has_lines": True,
    },
    "Quotations": {
        "primary_key": ["DocEntry", "LineNum"],
        "pii_columns": [],
        "scd_type": 1,
        "has_lines": True,
    },
    "InventoryGenEntries": {
        "primary_key": ["DocEntry", "LineNum"],
        "pii_columns": [],
        "scd_type": 1,
        "has_lines": True,
    },
}


def get_local_spark() -> SparkSession:
    """Spark local — sem Iceberg, sem Glue Catalog."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("debug-silver-local")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def bp(step: str, df, sample_rows: int = 5):
    """Breakpoint helper — imprime estado do DataFrame."""
    count = df.count()
    print(f"\n{'─'*60}")
    print(f"  ▸ [{step}] {count} registros | {len(df.columns)} colunas")
    print(f"{'─'*60}")
    df.printSchema()
    df.show(sample_rows, truncate=50)
    return df


def debug_dimension(spark, entity_name: str):
    """Debug passo a passo de uma dimensão."""
    cfg = ENTITIES_LOCAL[entity_name]
    path = os.path.join(SAMPLES_DIR, entity_name)

    print(f"\n{'='*60}")
    print(f"  DEBUG: {entity_name} (dimensão, SCD{cfg['scd_type']})")
    print(f"  Path: {path}")
    print(f"{'='*60}")

    # BP1: Leitura bruta
    df = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").json(path)
    df = df.withColumn("_source_file", input_file_name())
    df = bp("BP1 - Leitura bruta", df)

    # BP2: Campos disponíveis vs esperados
    print(f"\n  Colunas disponíveis ({len(df.columns)}):")
    for c in sorted(df.columns):
        print(f"    - {c}")

    # BP3: Clean strings (trim)
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    df = bp("BP3 - Após trim", df, 3)

    # BP4: Validação PK not null
    pk = cfg["primary_key"]
    condition = None
    for key in pk:
        key_check = col(key).isNotNull()
        condition = key_check if condition is None else (condition & key_check)

    antes = df.count()
    df = df.filter(condition)
    depois = df.count()
    print(f"\n  ▸ [BP4] PK nula removida: {antes - depois} registros descartados")

    # BP5: Dedup
    antes = df.count()
    w = Window.partitionBy(*pk).orderBy(desc("_source_file"))
    df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
    depois = df.count()
    print(f"  ▸ [BP5] Dedup: {antes} → {depois} (removidos: {antes - depois})")

    # BP6: Mask PII
    pii = cfg["pii_columns"]
    if pii:
        for c in pii:
            if c in df.columns:
                df = df.withColumn(c, when(col(c).isNotNull(), sha2(col(c).cast("string"), 256)).otherwise(lit(None)))
        print(f"  ▸ [BP6] PII mascarado: {pii}")
        df.select(*pk, *[c for c in pii if c in df.columns]).show(3, truncate=40)
    else:
        print(f"  ▸ [BP6] Sem PII para mascarar")

    # BP7: Row hash (SCD2)
    if cfg["scd_type"] == 2:
        exclude = {"_source_file", "_silver_loaded_at", "_silver_load_date", "_sk",
                   "_row_hash", "_valid_from", "_valid_to", "_is_current", "odata.etag"}
        hash_cols = sorted([c for c in df.columns if c not in exclude])
        df = df.withColumn(
            "_row_hash",
            sha2(concat_ws("||", *[coalesce(col(f"`{c}`").cast("string"), lit("__NULL__")) for c in hash_cols]), 256)
        )
        print(f"  ▸ [BP7] Hash SCD2 com {len(hash_cols)} colunas")
        df.select(*pk, "_row_hash").show(3, truncate=60)

    # BP8: Metadata
    df = df.withColumn("_silver_loaded_at", current_timestamp())
    df = df.withColumn("_sk", sha2(concat_ws("||", *[col(c).cast("string") for c in pk]), 256))
    df = df.drop("_source_file")
    df = bp("BP8 - Final (pronto para escrita)", df, 3)

    # Salva localmente para inspeção (Parquet)
    out_path = os.path.join(SAMPLES_DIR, "..", "output", entity_name)
    df.coalesce(1).write.mode("overwrite").parquet(out_path)
    print(f"\n  ✓ Output salvo em: {out_path}")

    return df


def debug_fact(spark, entity_name: str):
    """Debug passo a passo de uma fato (com explode de DocumentLines)."""
    cfg = ENTITIES_LOCAL[entity_name]
    path = os.path.join(SAMPLES_DIR, entity_name)

    print(f"\n{'='*60}")
    print(f"  DEBUG: {entity_name} (fato, lines explodido)")
    print(f"  Path: {path}")
    print(f"{'='*60}")

    # BP1: Leitura bruta
    df = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").json(path)
    df = df.withColumn("_source_file", input_file_name())
    df = bp("BP1 - Leitura bruta (headers)", df, 3)

    # BP2: Verifica se DocumentLines existe
    if "DocumentLines" not in df.columns:
        print("  ✗ ERRO: campo 'DocumentLines' não encontrado!")
        print(f"  Colunas disponíveis: {sorted(df.columns)}")
        return None

    lines_sample = df.select("DocEntry", "DocumentLines").first()
    print(f"  ▸ DocumentLines tipo: {df.schema['DocumentLines'].dataType}")
    print(f"  ▸ Exemplo DocEntry={lines_sample['DocEntry']}: {len(lines_sample['DocumentLines'] or [])} linhas")

    # BP3: Explode
    antes_headers = df.count()
    df_exploded = df.select("*", explode(col("DocumentLines")).alias("line")).drop("DocumentLines")
    depois_linhas = df_exploded.count()
    print(f"\n  ▸ [BP3] Explode: {antes_headers} headers → {depois_linhas} linhas")

    # BP4: Flatten line fields
    df_flat = df_exploded.select(
        col("DocEntry").cast("int"),
        col("DocNum").cast("int"),
        col("DocDate").cast("string"),
        col("CardCode").cast("string"),
        col("DocTotal").cast("double"),
        col("SalesPersonCode").cast("int"),
        col("DocumentStatus").cast("string"),
        col("CreationDate").cast("string"),
        col("_source_file"),
        col("line.LineNum").cast("int").alias("LineNum"),
        col("line.ItemCode").cast("string").alias("ItemCode"),
        col("line.Quantity").cast("double").alias("Quantity"),
        col("line.Price").cast("double").alias("UnitPrice"),
        col("line.LineTotal").cast("double").alias("LineTotal"),
        col("line.WarehouseCode").cast("string").alias("WarehouseCode"),
    )
    df_flat = bp("BP4 - Flatten (header + line)", df_flat, 5)

    # BP5: Dedup
    pk = cfg["primary_key"]
    antes = df_flat.count()
    w = Window.partitionBy(*pk).orderBy(desc("_source_file"))
    df_flat = df_flat.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
    depois = df_flat.count()
    print(f"  ▸ [BP5] Dedup: {antes} → {depois} (removidos: {antes - depois})")

    # BP6: Date partition
    df_flat = (
        df_flat
        .withColumn("DocDate", to_date(col("DocDate")))
        .withColumn("year", year(col("DocDate")))
        .withColumn("month", month(col("DocDate")))
    )
    df_flat.groupBy("year", "month").count().orderBy("year", "month").show()

    # BP7: Metadata + output
    df_flat = df_flat.withColumn("_silver_loaded_at", current_timestamp()).drop("_source_file")
    df_flat = bp("BP7 - Final", df_flat, 3)

    out_path = os.path.join(SAMPLES_DIR, "..", "output", entity_name)
    df_flat.coalesce(1).write.mode("overwrite").parquet(out_path)
    print(f"\n  ✓ Output salvo em: {out_path}")

    return df_flat


def main():
    parser = argparse.ArgumentParser(description="Debug local Silver ETL")
    parser.add_argument("--entity", required=True, help="Nome da entidade (ex: BusinessPartners, Invoices)")
    parser.add_argument("--all", action="store_true", help="Executa todas as entidades")
    args = parser.parse_args()

    spark = get_local_spark()
    spark.sparkContext.setLogLevel("WARN")

    if args.all:
        entities = list(ENTITIES_LOCAL.keys())
    else:
        entities = [args.entity]

    for entity in entities:
        if entity not in ENTITIES_LOCAL:
            print(f"Entidade desconhecida: {entity}")
            print(f"Disponíveis: {list(ENTITIES_LOCAL.keys())}")
            sys.exit(1)

        cfg = ENTITIES_LOCAL[entity]
        if cfg.get("has_lines"):
            debug_fact(spark, entity)
        else:
            debug_dimension(spark, entity)

    spark.stop()
    print("\n[DONE] Debug finalizado. Outputs em data/output/")


if __name__ == "__main__":
    main()
