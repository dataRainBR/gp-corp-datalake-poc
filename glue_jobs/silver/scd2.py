"""
Implementação SCD Type 2 para dimensões críticas (BusinessPartners, Items, SalesPersons).
Preserva histórico de alterações conforme requisito do briefing.

Colunas adicionadas:
- _valid_from: timestamp de início da validade
- _valid_to: timestamp de fim (NULL = versão corrente)
- _is_current: boolean flag de versão ativa
- _row_hash: hash SHA-256 para detecção de mudanças
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, sha2, concat_ws, coalesce
)
from pyspark.sql.types import BooleanType, TimestampType
from config import CATALOG_DATABASE_SILVER, ICEBERG_CATALOG


def compute_row_hash(df: DataFrame, exclude_cols: list = None) -> DataFrame:
    """
    Gera hash de todas as colunas de negócio para detectar mudanças.
    Exclui colunas de controle/metadata.
    """
    default_exclude = {
        "_silver_loaded_at", "_silver_load_date", "_sk",
        "_row_hash", "_valid_from", "_valid_to", "_is_current",
        "_source_file", "year", "month", "odata.etag"
    }
    exclude = default_exclude | set(exclude_cols or [])
    hash_cols = sorted([c for c in df.columns if c not in exclude])

    return df.withColumn(
        "_row_hash",
        sha2(concat_ws("||", *[coalesce(col(f"`{c}`").cast("string"), lit("__NULL__")) for c in hash_cols]), 256)
    )


def apply_scd2(spark: SparkSession, incoming_df: DataFrame, table_name: str, primary_key: list):
    """
    Aplica SCD Type 2:
    1. Registros novos → insere com _is_current=True
    2. Registros alterados → fecha versão anterior, insere nova versão
    3. Registros iguais → nenhuma ação (idempotência)
    """
    full_table = f"{ICEBERG_CATALOG}.{CATALOG_DATABASE_SILVER}.{table_name}"
    now = current_timestamp()

    # Computa hash do incoming
    incoming_hashed = compute_row_hash(incoming_df)

    # Primeira carga
    try:
        spark.table(full_table)
    except Exception:
        result = (
            incoming_hashed
            .withColumn("_valid_from", now)
            .withColumn("_valid_to", lit(None).cast(TimestampType()))
            .withColumn("_is_current", lit(True).cast(BooleanType()))
        )
        result.writeTo(full_table).using("iceberg").createOrReplace()
        print(f"[SCD2] Tabela {full_table} criada com {result.count()} registros (primeira carga).")
        return

    # Lê registros correntes existentes
    existing_current = spark.table(full_table).filter(col("_is_current") == True)

    # 1. Novos registros (não existem na tabela)
    new_records = (
        incoming_hashed.alias("inc")
        .join(existing_current.alias("ext"), on=primary_key, how="left_anti")
        .withColumn("_valid_from", now)
        .withColumn("_valid_to", lit(None).cast(TimestampType()))
        .withColumn("_is_current", lit(True).cast(BooleanType()))
    )

    # 2. Registros que mudaram (hash diferente)
    changed = (
        incoming_hashed.alias("inc")
        .join(existing_current.alias("ext"), on=primary_key, how="inner")
        .filter(col("inc._row_hash") != col("ext._row_hash"))
        .select([col(f"inc.`{c}`") for c in incoming_hashed.columns])
    )

    new_versions = (
        changed
        .withColumn("_valid_from", now)
        .withColumn("_valid_to", lit(None).cast(TimestampType()))
        .withColumn("_is_current", lit(True).cast(BooleanType()))
    )

    # 3. Fecha versões anteriores dos registros alterados
    changed_count = changed.count()
    if changed_count > 0:
        changed_keys_view = f"_changed_keys_{table_name}"
        changed.select(*[col(f"`{k}`") for k in primary_key]).createOrReplaceTempView(changed_keys_view)

        close_condition = " AND ".join([f"target.`{k}` = ck.`{k}`" for k in primary_key])
        spark.sql(f"""
            MERGE INTO {full_table} AS target
            USING {changed_keys_view} AS ck
            ON {close_condition} AND target._is_current = true
            WHEN MATCHED THEN UPDATE SET
                target._valid_to = current_timestamp(),
                target._is_current = false
        """)
        print(f"[SCD2] {changed_count} versões anteriores fechadas em {table_name}.")

    # 4. Insere novos + novas versões
    to_insert = new_records.unionByName(new_versions, allowMissingColumns=True)
    insert_count = to_insert.count()

    if insert_count > 0:
        to_insert.writeTo(full_table).append()
        print(f"[SCD2] {insert_count} registros inseridos em {table_name} "
              f"({new_records.count()} novos, {changed_count} atualizados).")
    else:
        print(f"[SCD2] Nenhuma alteração detectada para {table_name}. Sem ação.")
