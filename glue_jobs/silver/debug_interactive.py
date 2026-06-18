"""
Debug interativo das transformações Silver — roda passo a passo nos módulos reais.
Usa as funções reais de utils.py, scd2.py, job_dimensions.py.

USO:
  python glue_jobs/silver/debug_interactive.py --entity BusinessPartners

BREAKPOINTS (coloque no VS Code):
  - utils.py linha da função read_bronze_json()       → BP1
  - job_dimensions.py dentro de transform_*()         → BP2 (select/cast)
  - utils.py linha da função clean_strings()          → BP3
  - utils.py linha da função validate_not_null_keys() → BP4
  - utils.py linha da função deduplicate()            → BP5
  - utils.py linha da função mask_pii()               → BP6
  - utils.py linha da função add_silver_metadata()    → BP7
  - scd2.py linha da função compute_row_hash()        → BP8
  - scd2.py linha da função apply_scd2()              → BP9

Coloque breakpoints nessas funções e rode este script com F5 (Debug Python).
"""
import sys
import os

# Adiciona o diretório silver ao path para imports funcionarem
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
from pyspark.sql import SparkSession


# --- Override: substitui paths S3 por paths locais ---

SAMPLES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "samples")
SAMPLES_DIR = os.path.abspath(SAMPLES_DIR)


def patch_config():
    """Substitui paths S3 do config.py por paths locais."""
    import config

    for entity_name, cfg in config.ENTITIES.items():
        local_path = os.path.join(SAMPLES_DIR, entity_name, "")
        # Converte para formato file:// para Spark local
        cfg["bronze_path"] = local_path.replace("\\", "/")

    # Override do catalog (não vamos escrever no Glue real)
    config.ICEBERG_CATALOG = "local_catalog"
    config.CATALOG_DATABASE_SILVER = "debug_silver"


def get_local_spark() -> SparkSession:
    """Spark local sem Iceberg — para debug das transformações."""
    import tempfile

    # Workaround: evita dependência de winutils.exe no Windows
    hadoop_dir = os.path.join(tempfile.gettempdir(), "hadoop", "bin")
    os.makedirs(hadoop_dir, exist_ok=True)
    os.environ["HADOOP_HOME"] = os.path.dirname(hadoop_dir)
    os.environ["hadoop.home.dir"] = os.path.dirname(hadoop_dir)

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("debug-silver-interactive")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .getOrCreate()
    )


def override_spark_session():
    """Monkey-patch get_spark_session para retornar Spark local."""
    import utils
    spark = get_local_spark()
    spark.sparkContext.setLogLevel("WARN")
    utils.get_spark_session = lambda app_name: spark

    # Monkey-patch read_bronze_json para ler via pandas (evita Hadoop/winutils)
    original_read = utils.read_bronze_json

    def read_bronze_json_local(spark_session, base_path, load_type="all"):
        """Lê JSON local via pandas → Spark (sem Hadoop)."""
        import pandas as pd
        import json
        from glob import glob

        # Normaliza path
        path = base_path.replace("/", os.sep).rstrip(os.sep)
        json_files = glob(os.path.join(path, "**", "*.json"), recursive=True)
        if not json_files:
            json_files = glob(os.path.join(path, "*.json"))

        if not json_files:
            raise FileNotFoundError(f"Nenhum JSON encontrado em: {path}")

        print(f"       Lendo {len(json_files)} arquivo(s) de: {path}")

        all_records = []
        for jf in json_files:
            with open(jf, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for rec in data:
                    rec["_source_file"] = jf.replace(os.sep, "/")
                all_records.extend(data)
            elif isinstance(data, dict):
                data["_source_file"] = jf.replace(os.sep, "/")
                all_records.append(data)

        pdf = pd.json_normalize(all_records, max_level=0)
        df = spark_session.createDataFrame(pdf)
        return df

    utils.read_bronze_json = read_bronze_json_local
    return spark


def debug_dimension(spark, entity_name: str):
    """
    Executa a transformação real de uma dimensão usando os módulos de produção.
    Coloque breakpoints em cada função de utils.py e scd2.py.
    """
    import config
    from utils import (
        read_bronze_json,       # ← BP1: coloque breakpoint aqui
        clean_strings,          # ← BP3
        validate_not_null_keys, # ← BP4
        deduplicate,            # ← BP5
        mask_pii,               # ← BP6
        add_silver_metadata,    # ← BP7
        generate_surrogate_key,
    )
    from scd2 import compute_row_hash  # ← BP8

    cfg = config.ENTITIES[entity_name]

    print(f"\n{'='*60}")
    print(f"  DEBUG INTERATIVO: {entity_name}")
    print(f"  Bronze path: {cfg['bronze_path']}")
    print(f"  PK: {cfg['primary_key']}")
    print(f"  SCD: {cfg['scd_type']}")
    print(f"{'='*60}")

    # ═══ BP1: Leitura Bronze ═══
    print("\n[BP1] read_bronze_json()")
    df = read_bronze_json(spark, cfg["bronze_path"])  # ← BREAKPOINT
    print(f"       → {df.count()} registros, {len(df.columns)} colunas")
    df.printSchema()
    df.show(3, truncate=50)

    # ═══ BP2: Select/Cast (específico por entidade) ═══
    print("\n[BP2] Projeção e tipagem (veja transform_* em job_dimensions.py)")
    # Importa a função de transformação real
    from job_dimensions import TRANSFORM_MAP
    transform_fn = TRANSFORM_MAP[entity_name]
    df_transformed = transform_fn(spark, cfg)  # ← BREAKPOINT em job_dimensions.py
    print(f"       → {df_transformed.count()} registros após transformação completa")
    df_transformed.printSchema()
    df_transformed.show(5, truncate=50)

    # Se quiser inspecionar passo a passo dentro da transform_fn,
    # coloque breakpoints DENTRO de transform_business_partners() etc.

    print("\n[DONE] Transformação completa. Inspecione df_transformed.")
    print("       Para ver dados: df_transformed.toPandas()")

    return df_transformed


def debug_fact(spark, entity_name: str):
    """
    Executa a transformação real de uma fato (com explode).
    Coloque breakpoints em job_facts.py dentro de transform_invoices() etc.
    """
    import config
    from job_facts import TRANSFORM_MAP

    cfg = config.ENTITIES[entity_name]

    print(f"\n{'='*60}")
    print(f"  DEBUG INTERATIVO: {entity_name} (fato)")
    print(f"  Bronze path: {cfg['bronze_path']}")
    print(f"  PK: {cfg['primary_key']}")
    print(f"{'='*60}")

    # Executa transformação real
    transform_fn = TRANSFORM_MAP[entity_name]
    df = transform_fn(spark, cfg)  # ← BREAKPOINT em job_facts.py
    print(f"\n[RESULT] {df.count()} registros (linhas explodidas)")
    df.printSchema()
    df.show(5, truncate=50)

    # Distribuição por partição
    print("\n[PARTITIONS] year/month:")
    df.groupBy("year", "month").count().orderBy("year", "month").show()

    return df


def main():
    parser = argparse.ArgumentParser(description="Debug interativo Silver ETL (módulos reais)")
    parser.add_argument("--entity", default="BusinessPartners",
                        help="BusinessPartners, Items, ItemGroups, SalesPersons, Invoices, Orders, Quotations, InventoryGenEntries")
    args = parser.parse_args()

    # Patch paths e spark
    patch_config()
    spark = override_spark_session()

    import config
    entity = args.entity

    if entity not in config.ENTITIES:
        print(f"Entidade '{entity}' não encontrada. Disponíveis: {list(config.ENTITIES.keys())}")
        sys.exit(1)

    cfg = config.ENTITIES[entity]
    is_fact = cfg["scd_type"] == 1 and entity not in ("ItemGroups",)

    if is_fact and entity in ("Invoices", "Orders", "Quotations", "InventoryGenEntries"):
        df = debug_fact(spark, entity)
    else:
        df = debug_dimension(spark, entity)

    # Salva output local (parquet)
    out_path = os.path.join(SAMPLES_DIR, "..", "output", entity).replace("\\", "/")
    df.coalesce(1).write.mode("overwrite").parquet(out_path)
    print(f"\n[OUTPUT] Salvo em: {out_path}")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("\n" + "="*60)
        print("  ERRO CAPTURADO:")
        print("="*60)
        traceback.print_exc()
        print("="*60)
        input("Pressione Enter para sair...")
