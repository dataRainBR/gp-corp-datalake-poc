"""
AWS Glue Job: Silver → Gold — Feature Table para IA (CU-02)
Tabela: feature_win_rate

Objetivo: calcular features de conversão cotação → pedido por cliente/vendedor
para modelo preditivo de win-rate.

Nomenclatura Gold: snake_case com nomes de negócio (pt-BR).
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, countDistinct, avg, max as _max, min as _min,
    round as _round, datediff, current_date, current_timestamp, lit, when,
    coalesce
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


def build_feature_win_rate(spark):
    """
    Feature table: uma linha por combinação (cliente × vendedor).

    Features com nomenclatura de negócio:
    - total_cotacoes, total_pedidos, taxa_conversao
    - valor_medio_cotacao, valor_medio_pedido
    - dias_ciclo_venda (cotação → pedido)
    - recencia_ultima_cotacao, recencia_ultimo_pedido
    - diversidade_itens_cotados, diversidade_itens_pedidos
    """
    quotations = spark.table(silver_table("quotations"))
    orders     = spark.table(silver_table("orders"))
    bp = spark.table(silver_table("business_partners")).filter(col("_is_current") == True)
    sp = spark.table(silver_table("sales_persons")).filter(col("_is_current") == True)

    # --- Features de cotações por cliente/vendedor ---
    # Column pruning: seleciona só colunas necessárias ANTES do groupBy
    # Reduz I/O de ~10 GB para ~1 GB (Iceberg Parquet column projection)
    feat_cot = (
        quotations
        .select("cardcode", "salespersoncode", "docentry", "linetotal", "itemcode", "docdate")
        .groupBy("cardcode", "salespersoncode")
        .agg(
            countDistinct("docentry").alias("total_cotacoes"),
            _round(_sum("linetotal"), 2).alias("valor_total_cotacoes"),
            _round(avg("linetotal"), 2).alias("valor_medio_linha_cotacao"),
            countDistinct("itemcode").alias("diversidade_itens_cotados"),
            _max("docdate").alias("data_ultima_cotacao"),
            _min("docdate").alias("data_primeira_cotacao"),
        )
    )

    # --- Features de pedidos por cliente/vendedor ---
    feat_ped = (
        orders
        .select("cardcode", "salespersoncode", "docentry", "linetotal", "itemcode", "docdate")
        .groupBy("cardcode", "salespersoncode")
        .agg(
            countDistinct("docentry").alias("total_pedidos"),
            _round(_sum("linetotal"), 2).alias("valor_total_pedidos"),
            _round(avg("linetotal"), 2).alias("valor_medio_linha_pedido"),
            countDistinct("itemcode").alias("diversidade_itens_pedidos"),
            _max("docdate").alias("data_ultimo_pedido"),
            _min("docdate").alias("data_primeiro_pedido"),
        )
    )

    # --- Join e cálculo de win-rate ---
    df = (
        feat_cot.alias("c")
        .join(feat_ped.alias("p"), on=["cardcode", "salespersoncode"], how="left")
        .withColumn("total_pedidos", coalesce(col("total_pedidos"), lit(0)))
        .withColumn(
            "taxa_conversao",
            when(col("total_cotacoes") > 0,
                 _round(col("total_pedidos") / col("total_cotacoes"), 4))
            .otherwise(lit(0.0))
        )
        .withColumn(
            "dias_ciclo_venda",
            when(col("data_ultimo_pedido").isNotNull() & col("data_primeira_cotacao").isNotNull(),
                 datediff(col("data_ultimo_pedido"), col("data_primeira_cotacao")))
        )
        .withColumn("recencia_ultima_cotacao_dias", datediff(current_date(), col("data_ultima_cotacao")))
        .withColumn("recencia_ultimo_pedido_dias", datediff(current_date(), col("data_ultimo_pedido")))
    )

    # --- Enriquecer com dados do BP e vendedor ---
    df = (
        df
        .join(
            bp.select(
                col("cardcode"),
                col("cardtype").alias("tipo_cliente"),
                col("groupcode").alias("grupo_cliente"),
                col("creditlimit").alias("limite_credito"),
            ),
            on="cardcode", how="left"
        )
        .join(
            sp.select(
                col("salesemployeecode").alias("salespersoncode"),
                col("salesemployeename").alias("nome_vendedor"),
                col("commissionforsalesemployee").alias("pct_comissao_vendedor"),
            ),
            on="salespersoncode", how="left"
        )
        # Renomeia FKs restantes
        .withColumnRenamed("cardcode", "cod_cliente")
        .withColumnRenamed("salespersoncode", "cod_vendedor")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    # Escreve (full rebuild — tabela pequena)
    full_table = gold_table("features_predicao_conversao")
    parts = full_table.split(".")

    # Drop via boto3 direto — evita NoSuchKeyException do Iceberg
    import boto3
    try:
        boto3.client("glue", region_name="us-east-1").delete_table(
            DatabaseName=parts[1], Name=parts[2]
        )
        print(f"[DROP] features_predicao_conversao removida do catalog.")
    except Exception:
        pass  # Não existia — OK

    (
        df.writeTo(full_table)
        .using("iceberg")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .create()
    )
    print("[GOLD] features_predicao_conversao: escrita concluida (1 linha por cliente x vendedor).")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    spark = get_spark_session(args["JOB_NAME"])

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{GOLD_DATABASES['cotacoes']['database']}")

    print("\n" + "=" * 60)
    print("  GOLD — Features Predição Conversão (CU-02)")
    print("=" * 60)

    build_feature_win_rate(spark)

    spark.stop()
    print("\n[COMPLETE] Job Gold Features Predição Conversão finalizado.")


if __name__ == "__main__":
    main()
