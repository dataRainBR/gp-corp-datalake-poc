"""
AWS Glue Job: Iceberg Compaction — Silver
Reescreve data files fragmentados em arquivos maiores (~128 MB).

Problema: incrementais diários criam muitos small files (~1-5 MB cada).
         Spark gasta mais tempo abrindo arquivos do que processando.
         
Solução: CALL ... rewrite_data_files() consolida em menos arquivos.
         Rodar semanalmente (ex: domingo) ou quando job Gold demora > 15 min.

Custo: ~$0.22/run (2 workers G.1X, ~3 min por tabela grande)
Benefício: reduz tempo de leitura de quotations/orders de 15 min para ~3 min
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

from config import ENTITIES, CATALOG_DATABASE_SILVER, ICEBERG_CATALOG


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://gpcorp-datalake/Silver/warehouse")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )


def compact_table(spark, table_name: str, target_file_size_mb: int = 128):
    """
    Compacta data files de uma tabela Iceberg.
    Consolida small files em arquivos de ~target_file_size_mb.
    """
    full_table = f"{ICEBERG_CATALOG}.{CATALOG_DATABASE_SILVER}.{table_name}"
    target_bytes = target_file_size_mb * 1024 * 1024

    try:
        # Verifica se tabela existe
        spark.table(full_table)

        # Rewrite data files
        spark.sql(f"""
            CALL glue_catalog.system.rewrite_data_files(
                table => '{CATALOG_DATABASE_SILVER}.{table_name}',
                options => map(
                    'target-file-size-bytes', '{target_bytes}',
                    'min-file-size-bytes', '{int(target_bytes * 0.75)}',
                    'max-file-size-bytes', '{int(target_bytes * 1.8)}'
                )
            )
        """)
        print(f"[COMPACT] {table_name}: data files reescritos (target {target_file_size_mb} MB)")

        # Expire snapshots antigos (libera storage dos files antigos)
        spark.sql(f"""
            CALL glue_catalog.system.expire_snapshots(
                table => '{CATALOG_DATABASE_SILVER}.{table_name}',
                older_than => TIMESTAMP '{_get_expire_timestamp()}',
                retain_last => 3
            )
        """)
        print(f"[COMPACT] {table_name}: snapshots antigos expirados")

        # Remove orphan files
        spark.sql(f"""
            CALL glue_catalog.system.remove_orphan_files(
                table => '{CATALOG_DATABASE_SILVER}.{table_name}',
                older_than => TIMESTAMP '{_get_expire_timestamp()}'
            )
        """)
        print(f"[COMPACT] {table_name}: orphan files removidos")

    except Exception as e:
        print(f"[WARN] Compaction falhou para {table_name}: {e}")


def _get_expire_timestamp():
    """Retorna timestamp de 7 dias atrás (formato Iceberg)."""
    from datetime import datetime, timedelta
    return (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    spark = get_spark_session(args["JOB_NAME"])

    # Entidades a compactar (fatos são as mais fragmentadas)
    try:
        resolved = getResolvedOptions(sys.argv, ["entities"])
        entities = resolved["entities"].split(",")
    except Exception:
        # Default: tabelas menores e mais fragmentadas (incrementais puros)
        # Invoices e Orders são grandes (8-10 GB full) — compaction travava
        # Quotations, dimensões e InventoryGenEntries são puramente incrementais
        entities = ["quotations", "inventory_gen_entries",
                    "business_partners", "items", "sales_persons", "item_groups"]

    print("\n" + "=" * 60)
    print("  SILVER — Iceberg Compaction")
    print("=" * 60)

    for table_name in entities:
        table_name = table_name.strip()
        print(f"\n[TABLE] {table_name}")
        compact_table(spark, table_name)

    spark.stop()
    print("\n[COMPLETE] Compaction finalizada.")


if __name__ == "__main__":
    main()
