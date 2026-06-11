"""
Módulo de escrita Iceberg — MERGE (upsert) e criação de tabelas.
Garante idempotência: reprocessar não duplica dados.
"""
from pyspark.sql import DataFrame, SparkSession
from config import CATALOG_DATABASE_SILVER, ICEBERG_CATALOG


def write_iceberg_merge(spark: SparkSession, df: DataFrame, table_name: str,
                        primary_key: list, partition_by: list = None):
    """
    MERGE INTO (upsert) para tabelas Iceberg via Glue Catalog.
    - Se tabela não existe: cria com CREATE OR REPLACE
    - Se tabela existe: MERGE por PK (insert ou update)
    """
    full_table = f"{ICEBERG_CATALOG}.{CATALOG_DATABASE_SILVER}.{table_name}"

    if _table_exists(spark, full_table):
        _merge_into(spark, df, full_table, primary_key)
    else:
        _create_table(spark, df, full_table, partition_by)


def _table_exists(spark: SparkSession, full_table: str) -> bool:
    """Verifica se a tabela Iceberg já existe no Glue Catalog."""
    try:
        spark.table(full_table)
        return True
    except Exception:
        return False


def _create_table(spark: SparkSession, df: DataFrame, full_table: str, partition_by: list = None):
    """Cria tabela Iceberg pela primeira vez."""
    writer = df.writeTo(full_table).using("iceberg")

    if partition_by:
        # Iceberg partition spec
        from pyspark.sql.functions import col
        writer = writer.partitionedBy(*partition_by)

    writer.createOrReplace()
    count = df.count()
    print(f"[WRITE] Tabela {full_table} criada com {count} registros.")


def _merge_into(spark: SparkSession, df: DataFrame, full_table: str, primary_key: list):
    """
    Executa MERGE INTO: atualiza existentes, insere novos.
    Usa Spark SQL com Iceberg extensions.
    """
    temp_view = f"_incoming_{full_table.split('.')[-1]}"
    df.createOrReplaceTempView(temp_view)

    # Constrói a condição de JOIN
    merge_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in primary_key])

    # Colunas para UPDATE (exclui PKs)
    update_cols = [c for c in df.columns if c not in primary_key]
    update_set = ", ".join([f"target.`{c}` = source.`{c}`" for c in update_cols])

    # Colunas para INSERT
    all_cols = ", ".join([f"`{c}`" for c in df.columns])
    insert_vals = ", ".join([f"source.`{c}`" for c in df.columns])

    merge_sql = f"""
        MERGE INTO {full_table} AS target
        USING {temp_view} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({insert_vals})
    """

    spark.sql(merge_sql)
    print(f"[WRITE] MERGE concluído em {full_table}.")
