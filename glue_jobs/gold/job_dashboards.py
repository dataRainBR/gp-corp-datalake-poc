"""
AWS Glue Job: Silver → Gold — Agregações para Dashboards (CU-01)

Dashboards implementados:
1. Vendas: faturamento por período, vendedor, cliente, produto, filial
2. Cotações: volume, taxa de conversão por vendedor e linha de produto
3. Performance: ranking, ticket médio, mix

Consumidor: QuickSight via Athena.
Nomenclatura Gold: snake_case em português.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, countDistinct, avg, max as _max, min as _min,
    round as _round, year, month, current_timestamp, lit, when, coalesce,
    dense_rank
)
from pyspark.sql.window import Window

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


# ═══════════════════════════════════════════════════════════════
# VENDAS DETALHADA — grão linha de NF (drill-down)
# ═══════════════════════════════════════════════════════════════

def build_vendas_detalhada(spark, invoices, vendedores, items, item_groups):
    """
    Tabela flat desnormalizada — uma linha por item de NF.
    Base para drill-down dos dashboards (QuickSight).
    Recebe DataFrames já em cache — sem releitura.
    """

    df = (
        invoices
        .select(
            col("docentry").alias("num_doc"),
            col("docnum").alias("num_nota"),
            col("linenum").alias("num_linha"),
            col("docdate").alias("data_nota"),
            col("year").alias("ano"),
            col("month").alias("mes"),
            col("day").alias("dia"),
            col("cardcode").alias("cod_cliente"),
            col("cardname").alias("nome_cliente"),
            col("salespersoncode").alias("cod_vendedor"),
            col("itemcode").alias("cod_item"),
            col("itemdescription").alias("desc_item"),
            col("quantity").alias("quantidade"),
            col("unitprice").alias("preco_unitario"),
            col("linetotal").alias("valor_linha"),
            col("valortotallinha").alias("valor_total"),
            col("grossprofit").alias("lucro_bruto"),
            col("linediscountpercent").alias("pct_desconto"),
            col("warehousecode").alias("cod_filial"),
            col("nomefilial").alias("nome_filial"),
            col("cfopcode").alias("cfop"),
            col("linestatus").alias("status_linha"),
            col("branchid").alias("cod_filial_empresa"),
            col("numberofinstallments").alias("num_parcelas"),
            col("documentadditionalexpenses").alias("despesas_adicionais"),
        )
    )

    # Enriquecer com nome do vendedor
    df = df.join(
        vendedores.select(
            col("salesemployeecode").alias("cod_vendedor"),
            col("salesemployeename").alias("nome_vendedor"),
        ),
        on="cod_vendedor", how="left"
    )

    # Enriquecer com grupo do item + marca
    df = df.join(
        items.select(
            col("itemcode").alias("cod_item"),
            col("itemsgroupcode").alias("cod_grupo_item"),
            col("marca"),
        ),
        on="cod_item", how="left"
    )

    df = df.join(
        item_groups.select(
            col("number").alias("cod_grupo_item"),
            col("groupname").alias("nome_grupo_item"),
        ),
        on="cod_grupo_item", how="left"
    )

    df = df.withColumn("_gold_loaded_at", current_timestamp())
    _write_gold(spark, df, "vendas_detalhada", ["ano", "mes", "dia"])
    print("[GOLD] vendas_detalhada: escrita concluida.")


# ═══════════════════════════════════════════════════════════════
# Dashboard 1: VENDAS — faturamento por período, vendedor, cliente, produto, filial
# ═══════════════════════════════════════════════════════════════

def build_faturamento_mensal(spark, invoices):
    """
    Faturamento mensal agregado por vendedor, cliente, produto e filial.
    Grão: ano/mes × vendedor × cliente × produto × filial.
    Recebe invoices já em cache — evita releitura.
    """
    df = (
        invoices
        .groupBy("year", "month", "salespersoncode", "cardcode", "cardname",
                 "itemcode", "itemdescription", "warehousecode")
        .agg(
            _sum("linetotal").alias("receita_total"),
            _sum("quantity").alias("qtd_itens"),
            countDistinct("docentry").alias("qtd_notas"),
            _round(avg("unitprice"), 2).alias("preco_medio"),
            _round(avg("linediscountpercent"), 2).alias("pct_desconto_medio"),
            _sum("grossprofit").alias("lucro_bruto"),
            _min("docdate").alias("primeira_venda_periodo"),
            _max("docdate").alias("ultima_venda_periodo"),
        )
        .withColumnRenamed("year", "ano")
        .withColumnRenamed("month", "mes")
        .withColumnRenamed("salespersoncode", "cod_vendedor")
        .withColumnRenamed("cardcode", "cod_cliente")
        .withColumnRenamed("cardname", "nome_cliente")
        .withColumnRenamed("itemcode", "cod_item")
        .withColumnRenamed("itemdescription", "desc_item")
        .withColumnRenamed("warehousecode", "cod_filial")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    _write_gold(spark, df, "faturamento_mensal", ["ano", "mes"])
    print("[GOLD] faturamento_mensal: escrita concluida.")


# ═══════════════════════════════════════════════════════════════
# Dashboard 2: COTAÇÕES — volume, taxa de conversão por vendedor e linha
# ═══════════════════════════════════════════════════════════════

def build_taxa_conversao(spark, vendedores=None):
    """
    Taxa de conversão cotação → pedido.
    vendedores: DataFrame em cache (opcional, lê do Silver se None).
    """
    if vendedores is None:
        vendedores = spark.table(silver_table("sales_persons")).filter(col("_is_current") == True)
    quotations = spark.table(silver_table("quotations"))
    orders = spark.table(silver_table("orders")) if _table_exists(spark, silver_table("orders")) else None

    # Volume de cotações por vendedor/item/mês
    cot = (
        quotations
        .groupBy("year", "month", "salespersoncode", "itemcode", "itemdescription")
        .agg(
            countDistinct("docentry").alias("total_cotacoes"),
            _sum("linetotal").alias("valor_cotacoes"),
            _sum("quantity").alias("qtd_cotada"),
        )
    )

    if orders is not None:
        # Pedidos por vendedor/item/mês
        ped = (
            orders
            .groupBy(
                year("docdate").alias("year"),
                month("docdate").alias("month"),
                col("salespersoncode"),
                col("itemcode")
            )
            .agg(
                countDistinct("docentry").alias("total_pedidos"),
                _sum("linetotal").alias("valor_pedidos"),
                _sum("quantity").alias("qtd_pedida"),
            )
        )

        df = (
            cot
            .join(ped, on=["year", "month", "salespersoncode", "itemcode"], how="left")
            .withColumn("total_pedidos", coalesce(col("total_pedidos"), lit(0)))
            .withColumn(
                "taxa_conversao",
                when(col("total_cotacoes") > 0,
                     _round(col("total_pedidos") / col("total_cotacoes"), 4))
                .otherwise(lit(0.0))
            )
        )
    else:
        df = cot.withColumn("total_pedidos", lit(0)).withColumn("taxa_conversao", lit(0.0))

    # Enriquecer com nome do vendedor (usa vendedores do cache)
    df = (
        df
        .join(
            vendedores.select(
                col("salesemployeecode").alias("salespersoncode"),
                col("salesemployeename")
            ),
            on="salespersoncode", how="left"
        )
        .withColumnRenamed("year", "ano")
        .withColumnRenamed("month", "mes")
        .withColumnRenamed("salespersoncode", "cod_vendedor")
        .withColumnRenamed("salesemployeename", "nome_vendedor")
        .withColumnRenamed("itemcode", "cod_item")
        .withColumnRenamed("itemdescription", "desc_item")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    _write_gold(spark, df, "taxa_conversao", [])
    print("[GOLD] taxa_conversao: escrita concluida.")


# ═══════════════════════════════════════════════════════════════
# Dashboard 3: PERFORMANCE — ranking, ticket médio, mix
# ═══════════════════════════════════════════════════════════════

def build_ranking_vendedores(spark, invoices, vendedores):
    """
    Ranking de vendedores por mês.
    Recebe invoices e vendedores em cache — evita releitura.
    """
    perf = (
        invoices
        .groupBy("year", "month", "salespersoncode")
        .agg(
            _sum("linetotal").alias("receita_faturada"),
            _sum("grossprofit").alias("lucro_bruto"),
            countDistinct("docentry").alias("qtd_notas"),
            countDistinct("cardcode").alias("qtd_clientes"),
            countDistinct("itemcode").alias("mix_produtos"),
            _sum("quantity").alias("qtd_itens_vendidos"),
            _round(avg("linetotal"), 2).alias("ticket_medio_linha"),
            _round(
                _sum("grossprofit") / _sum("linetotal") * 100, 2
            ).alias("pct_margem_bruta"),
        )
    )

    # Ranking por receita
    w = Window.partitionBy("year", "month").orderBy(col("receita_faturada").desc())
    df = (
        perf
        .withColumn("ranking", dense_rank().over(w))
        .join(
            vendedores.select(
                col("salesemployeecode").alias("salespersoncode"),
                col("salesemployeename")
            ),
            on="salespersoncode", how="left"
        )
        .withColumnRenamed("year", "ano")
        .withColumnRenamed("month", "mes")
        .withColumnRenamed("salespersoncode", "cod_vendedor")
        .withColumnRenamed("salesemployeename", "nome_vendedor")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    _write_gold(spark, df, "ranking_vendedores", ["ano", "mes"])
    print("[GOLD] ranking_vendedores: escrita concluida.")


def build_vendas_por_produto(spark, invoices):
    """
    Vendas por produto/grupo — base para análise ABC e mix.
    Recebe invoices em cache — evita releitura.
    """
    items = spark.table(silver_table("items")).filter(col("_is_current") == True)
    item_groups = spark.table(silver_table("item_groups"))

    vendas = (
        invoices
        .groupBy("year", "month", "itemcode")
        .agg(
            _sum("linetotal").alias("receita"),
            _sum("quantity").alias("quantidade"),
            _sum("grossprofit").alias("lucro_bruto"),
            countDistinct("docentry").alias("qtd_notas"),
            countDistinct("cardcode").alias("qtd_clientes"),
            countDistinct("salespersoncode").alias("qtd_vendedores"),
            _round(avg("unitprice"), 2).alias("preco_medio"),
        )
    )

    df = (
        vendas
        .join(
            items.select("itemcode", "itemname", "itemsgroupcode"),
            on="itemcode", how="left"
        )
        .join(
            item_groups.select(col("number").alias("itemsgroupcode"), col("groupname")),
            on="itemsgroupcode", how="left"
        )
        .withColumnRenamed("year", "ano")
        .withColumnRenamed("month", "mes")
        .withColumnRenamed("itemcode", "cod_item")
        .withColumnRenamed("itemname", "nome_item")
        .withColumnRenamed("itemsgroupcode", "cod_grupo_item")
        .withColumnRenamed("groupname", "nome_grupo_item")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    _write_gold(spark, df, "vendas_por_produto", ["ano", "mes"])
    print("[GOLD] vendas_por_produto: escrita concluida.")


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

def _table_exists(spark, full_table: str) -> bool:
    try:
        spark.table(full_table)
        return True
    except Exception:
        return False


def _write_gold(spark, df, table_name: str, partition_by: list):
    """
    Escreve tabela Gold Iceberg.
    Usa DELETE TABLE via boto3 diretamente — evita o ciclo drop→S3 error→boto3 fallback.
    """
    import boto3
    full_table = gold_table(table_name)
    parts = full_table.split(".")  # glue_catalog.database.table
    database = parts[1]
    table = parts[2]

    # Remove do catalog via boto3 diretamente — evita NoSuchKeyException do Iceberg
    try:
        boto3.client("glue", region_name="us-east-1").delete_table(
            DatabaseName=database, Name=table
        )
        print(f"[DROP] {table_name} removida do catalog.")
    except Exception:
        pass  # Não existia — OK

    if partition_by:
        df = df.sortWithinPartitions(*partition_by)
        writer = (
            df.writeTo(full_table)
            .using("iceberg")
            .partitionedBy(*partition_by)
            .tableProperty("write.spark.fanout.enabled", "true")
            .tableProperty("write.parquet.compression-codec", "zstd")
        )
    else:
        writer = (
            df.writeTo(full_table)
            .using("iceberg")
            .tableProperty("write.parquet.compression-codec", "zstd")
        )

    writer.create()


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    spark = get_spark_session(args["JOB_NAME"])

    # Cria databases Gold por domínio
    for domain, cfg in GOLD_DATABASES.items():
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{cfg['database']}")

    print("\n" + "=" * 60)
    print("  GOLD — Dashboards Comerciais (CU-01)")
    print("=" * 60)

    # ── Carrega e cacheia tabelas Silver UMA VEZ ──
    # invoices: usada em vendas_detalhada + faturamento_mensal + vendas_por_produto + ranking
    print("\n[LOAD] Carregando Silver em cache...")
    invoices = spark.table(silver_table("invoices")).cache()
    invoices.count()  # materializa o cache

    vendedores = spark.table(silver_table("sales_persons")).filter(col("_is_current") == True).cache()
    items      = spark.table(silver_table("items")).filter(col("_is_current") == True).cache()
    item_groups = spark.table(silver_table("item_groups")).cache()

    print("[LOAD] Cache pronto.\n")

    # Dashboard 1: Vendas
    print("[DASHBOARD] Vendas")
    build_vendas_detalhada(spark, invoices, vendedores, items, item_groups)
    build_faturamento_mensal(spark, invoices)
    build_vendas_por_produto(spark, invoices)

    # Dashboard 2: Cotações
    print("\n[DASHBOARD] Cotações")
    build_taxa_conversao(spark, vendedores)

    # Dashboard 3: Performance
    print("\n[DASHBOARD] Performance Equipe Comercial")
    build_ranking_vendedores(spark, invoices, vendedores)

    # Libera cache
    invoices.unpersist()
    vendedores.unpersist()
    items.unpersist()
    item_groups.unpersist()

    spark.stop()
    print("\n[COMPLETE] Job Gold Dashboards finalizado.")


if __name__ == "__main__":
    main()
