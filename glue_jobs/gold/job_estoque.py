"""
AWS Glue Job: Silver → Gold — Movimentação de Estoque
Domínio: estoque (gpcorp_gold_estoque)
Tabela: movimentacao_estoque

Agrega entradas de mercadoria (InventoryGenEntries) por mês/item/depósito.
Consumidor: QuickSight via Athena.
Nomenclatura Gold: snake_case em português.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, countDistinct, avg, max as _max, min as _min,
    round as _round, year, month, current_timestamp, lit, when, abs as _abs
)

from config import (
    CATALOG_DATABASE_SILVER, ICEBERG_CATALOG,
    GOLD_DATABASES, get_gold_table_path, get_gold_warehouse
)


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://gpcorp-datalake/Gold/")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )


def silver_table(name: str) -> str:
    return f"{ICEBERG_CATALOG}.{CATALOG_DATABASE_SILVER}.{name}"


def gold_table(name: str) -> str:
    return get_gold_table_path(name)


def build_movimentacao_estoque(spark):
    """
    Movimentação de estoque agregada por mês/item/depósito.
    InventoryGenEntries são entradas de mercadoria (OIGN no SAP B1).
    Quantity > 0 = entrada, Quantity < 0 = saída (ajuste negativo).

    Grão: ano/mes × item × depósito.
    """
    ige = spark.table(silver_table("inventory_gen_entries"))
    items = spark.table(silver_table("items")).filter(col("_is_current") == True)
    item_groups = spark.table(silver_table("item_groups"))

    # Classificar entrada vs saída pela quantidade
    ige_classified = (
        ige
        .withColumn("qtd_entrada", when(col("quantity") > 0, col("quantity")).otherwise(lit(0)))
        .withColumn("qtd_saida", when(col("quantity") < 0, _abs(col("quantity"))).otherwise(lit(0)))
        .withColumn("valor_entrada", when(col("quantity") > 0, col("linetotal")).otherwise(lit(0)))
        .withColumn("valor_saida", when(col("quantity") < 0, _abs(col("linetotal"))).otherwise(lit(0)))
    )

    # Agregar por mês/item/depósito
    df = (
        ige_classified
        .groupBy("year", "month", "ItemCode", "WarehouseCode")
        .agg(
            _sum("qtd_entrada").alias("qtd_entrada"),
            _sum("qtd_saida").alias("qtd_saida"),
            _sum("valor_entrada").alias("valor_entrada"),
            _sum("valor_saida").alias("valor_saida"),
            _sum("Quantity").alias("saldo_qtd"),
            _sum("LineTotal").alias("saldo_valor"),
            countDistinct("DocEntry").alias("qtd_documentos"),
            count("*").alias("qtd_linhas"),
            _round(avg("UnitPrice"), 2).alias("preco_medio_unitario"),
            _min("DocDate").alias("primeira_movimentacao"),
            _max("DocDate").alias("ultima_movimentacao"),
        )
    )

    # Enriquecer com dados do item e grupo
    df = (
        df
        .join(
            items.select("ItemCode", "ItemName", "ItemsGroupCode"),
            on="ItemCode", how="left"
        )
        .join(
            item_groups.select(
                col("number").alias("itemsgroupcode"),
                col("groupname")
            ),
            on="itemsgroupcode", how="left"
        )
        .withColumnRenamed("year", "ano")
        .withColumnRenamed("month", "mes")
        .withColumnRenamed("ItemCode", "cod_item")
        .withColumnRenamed("ItemName", "nome_item")
        .withColumnRenamed("ItemsGroupCode", "cod_grupo_item")
        .withColumnRenamed("GroupName", "nome_grupo_item")
        .withColumnRenamed("WarehouseCode", "cod_deposito")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    # Escrever
    full_table = gold_table("movimentacao_estoque")
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    df = df.sortWithinPartitions("ano", "mes")
    writer = (
        df.writeTo(full_table)
        .using("iceberg")
        .partitionedBy("ano", "mes")
        .tableProperty("write.spark.fanout.enabled", "true")
    )
    writer.createOrReplace()
    print(f"[GOLD] movimentacao_estoque: {df.count()} registros.")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    spark = get_spark_session(args["JOB_NAME"])

    # Cria database do domínio estoque
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{GOLD_DATABASES['estoque']['database']}"
    )

    print("\n" + "=" * 60)
    print("  GOLD — Movimentação de Estoque")
    print("=" * 60)

    build_movimentacao_estoque(spark)

    spark.stop()
    print("\n[COMPLETE] Job Gold Estoque finalizado.")


if __name__ == "__main__":
    main()
