"""
AWS Glue Job: Silver → Gold — Domínio Cadastros
Database: gpcorp_gold_cadastros
Tabelas:
  1. perfil_cliente — RFV + segmentação ABC + atividade
  2. catalogo_produtos_ativo — catálogo enriquecido com métricas de vendas
  3. cobertura_vendedor — carteira por vendedor, cobertura, performance

Fonte principal: invoices (column pruning) + dimensões (BP, Items, SalesPersons)
Estratégia: carrega invoices UMA VEZ em cache, gera as 3 tabelas
Custo estimado: ~$0.30/run (2 workers G.1X × 5 min)
"""
import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, countDistinct, avg, max as _max, min as _min,
    round as _round, datediff, current_date, current_timestamp, lit, when,
    percent_rank, months_between, concat_ws
)
from pyspark.sql.window import Window

from config import (
    CATALOG_DATABASE_SILVER, ICEBERG_CATALOG,
    GOLD_DATABASES, get_gold_table_path
)

glue_client = boto3.client("glue", region_name="us-east-1")


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


def write_gold(spark, df, table_name: str):
    """Escreve tabela Gold (drop via boto3 + create)."""
    full_table = get_gold_table_path(table_name)
    parts = full_table.split(".")
    try:
        glue_client.delete_table(DatabaseName=parts[1], Name=parts[2])
        print(f"  [DROP] {table_name}")
    except Exception:
        pass
    (
        df.writeTo(full_table)
        .using("iceberg")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .create()
    )
    print(f"  [OK] {table_name}: escrita concluida.")


# ═══════════════════════════════════════════════════════════════
# 1. PERFIL CLIENTE — RFV + Segmentação
# ═══════════════════════════════════════════════════════════════

def build_perfil_cliente(spark, invoices, bp):
    """
    Uma linha por cliente com métricas RFV e classificação ABC.
    """
    print("\n[1] perfil_cliente (RFV + ABC)")

    rfv = (
        invoices
        .filter(col("cancelled") != "tYES")
        .groupBy("cardcode")
        .agg(
            _max("docdate").alias("data_ultima_compra"),
            _min("docdate").alias("data_primeira_compra"),
            datediff(current_date(), _max("docdate")).alias("recencia_dias"),
            countDistinct("docentry").alias("total_notas"),
            countDistinct(col("docdate").substr(1, 7)).alias("meses_ativos"),
            count("*").alias("total_linhas"),
            _round(_sum("linetotal"), 2).alias("valor_total_compras"),
            _round(avg("linetotal"), 2).alias("ticket_medio_linha"),
            _round(_sum("grossprofit"), 2).alias("lucro_bruto_total"),
            countDistinct("itemcode").alias("diversidade_itens"),
            countDistinct("salespersoncode").alias("vendedores_atenderam"),
        )
    )

    # Classificação ABC
    w_abc = Window.orderBy(col("valor_total_compras").desc())
    rfv = (
        rfv
        .withColumn("pct_rank_receita", _round(percent_rank().over(w_abc), 4))
        .withColumn(
            "classificacao_abc",
            when(col("pct_rank_receita") <= 0.2, lit("A"))
            .when(col("pct_rank_receita") <= 0.5, lit("B"))
            .otherwise(lit("C"))
        )
        .withColumn(
            "pct_margem_media",
            when(col("valor_total_compras") > 0,
                 _round(col("lucro_bruto_total") / col("valor_total_compras") * 100, 2))
            .otherwise(lit(0.0))
        )
        .withColumn(
            "meses_desde_primeira_compra",
            _round(months_between(current_date(), col("data_primeira_compra")), 0).cast("int")
        )
        .withColumn(
            "frequencia_mensal",
            when(col("meses_desde_primeira_compra") > 0,
                 _round(col("total_notas") / col("meses_desde_primeira_compra"), 2))
            .otherwise(col("total_notas").cast("double"))
        )
        .withColumn(
            "segmento_atividade",
            when(col("recencia_dias") <= 30, lit("ativo"))
            .when(col("recencia_dias") <= 90, lit("em_risco"))
            .when(col("recencia_dias") <= 180, lit("inativo_recente"))
            .otherwise(lit("inativo_cronico"))
        )
    )

    df = (
        rfv.join(bp, on="cardcode", how="left")
        .withColumnRenamed("cardcode", "cod_cliente")
        .withColumnRenamed("cardtype", "tipo_cliente")
        .withColumnRenamed("groupcode", "grupo_cliente")
        .withColumnRenamed("creditlimit", "limite_credito")
        .withColumnRenamed("currentaccountbalance", "saldo_conta_corrente")
        .withColumnRenamed("city", "cidade")
        .withColumnRenamed("billtostate", "uf")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    write_gold(spark, df, "perfil_cliente")


# ═══════════════════════════════════════════════════════════════
# 2. CATÁLOGO PRODUTOS ATIVO — enriquecido com vendas
# ═══════════════════════════════════════════════════════════════

def build_catalogo_produtos(spark, invoices, items, item_groups):
    """
    Uma linha por produto com: vendeu nos últimos 30/90/180 dias?
    Preço médio praticado, qtd clientes, tendência.
    """
    print("\n[2] catalogo_produtos_ativo")

    # Métricas de vendas por item
    vendas_item = (
        invoices
        .filter(col("cancelled") != "tYES")
        .groupBy("itemcode")
        .agg(
            _sum("linetotal").alias("receita_total"),
            _sum("quantity").alias("qtd_total_vendida"),
            _round(avg("unitprice"), 2).alias("preco_medio_praticado"),
            _min("unitprice").alias("preco_minimo"),
            _max("unitprice").alias("preco_maximo"),
            countDistinct("cardcode").alias("qtd_clientes_compraram"),
            countDistinct("docentry").alias("qtd_notas"),
            _max("docdate").alias("data_ultima_venda"),
            _min("docdate").alias("data_primeira_venda"),
            datediff(current_date(), _max("docdate")).alias("dias_sem_venda"),
        )
    )

    # Vendas recentes (flags de atividade)
    vendas_30d = (
        invoices
        .filter((col("cancelled") != "tYES") & (datediff(current_date(), col("docdate")) <= 30))
        .groupBy("itemcode")
        .agg(
            _sum("linetotal").alias("receita_30d"),
            _sum("quantity").alias("qtd_30d"),
        )
    )

    vendas_90d = (
        invoices
        .filter((col("cancelled") != "tYES") & (datediff(current_date(), col("docdate")) <= 90))
        .groupBy("itemcode")
        .agg(
            _sum("linetotal").alias("receita_90d"),
            _sum("quantity").alias("qtd_90d"),
        )
    )

    # Join com dimensões
    df = (
        items
        .select("itemcode", "itemname", "foreignname", "itemsgroupcode",
                "quantityonstock", "valid", "salesunit", "purchaseunit", "marca")
        .join(vendas_item, on="itemcode", how="left")
        .join(vendas_30d, on="itemcode", how="left")
        .join(vendas_90d, on="itemcode", how="left")
        .join(
            item_groups.select(col("number").alias("itemsgroupcode"), col("groupname")),
            on="itemsgroupcode", how="left"
        )
        .withColumn("vendeu_30d", col("receita_30d").isNotNull())
        .withColumn("vendeu_90d", col("receita_90d").isNotNull())
        .withColumn(
            "status_produto",
            when(col("valid") == "tYES",
                 when(col("dias_sem_venda").isNull(), lit("sem_venda"))
                 .when(col("dias_sem_venda") <= 30, lit("ativo"))
                 .when(col("dias_sem_venda") <= 90, lit("lento"))
                 .when(col("dias_sem_venda") <= 180, lit("encalhado"))
                 .otherwise(lit("obsoleto"))
            ).otherwise(lit("inativo_cadastro"))
        )
        # ABC por receita de produto
        .withColumn(
            "classificacao_abc_produto",
            when(col("receita_total").isNull(), lit("sem_venda"))
            .otherwise(lit(""))  # será calculado abaixo
        )
        .withColumnRenamed("itemcode", "cod_item")
        .withColumnRenamed("itemname", "nome_item")
        .withColumnRenamed("foreignname", "nome_estrangeiro")
        .withColumnRenamed("itemsgroupcode", "cod_grupo")
        .withColumnRenamed("groupname", "nome_grupo")
        .withColumnRenamed("quantityonstock", "estoque_atual")
        .withColumnRenamed("salesunit", "unidade_venda")
        .withColumnRenamed("purchaseunit", "unidade_compra")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    # ABC produto
    w_abc = Window.orderBy(col("receita_total").desc())
    df = (
        df
        .withColumn("_rank", percent_rank().over(w_abc))
        .withColumn(
            "classificacao_abc_produto",
            when(col("receita_total").isNull(), lit("sem_venda"))
            .when(col("_rank") <= 0.2, lit("A"))
            .when(col("_rank") <= 0.5, lit("B"))
            .otherwise(lit("C"))
        )
        .drop("_rank")
    )

    write_gold(spark, df, "catalogo_produtos_ativo")


# ═══════════════════════════════════════════════════════════════
# 3. COBERTURA VENDEDOR — carteira e performance
# ═══════════════════════════════════════════════════════════════

def build_cobertura_vendedor(spark, invoices, vendedores):
    """
    Uma linha por vendedor com métricas de carteira:
    clientes ativos, inativos, mix, receita, ticket, cobertura.
    """
    print("\n[3] cobertura_vendedor")

    # Métrica por vendedor
    perf = (
        invoices
        .filter(col("cancelled") != "tYES")
        .groupBy("salespersoncode")
        .agg(
            _round(_sum("linetotal"), 2).alias("receita_total"),
            _round(_sum("grossprofit"), 2).alias("lucro_bruto_total"),
            countDistinct("docentry").alias("total_notas"),
            countDistinct("cardcode").alias("total_clientes"),
            countDistinct("itemcode").alias("mix_produtos"),
            _round(avg("linetotal"), 2).alias("ticket_medio_linha"),
            _max("docdate").alias("data_ultima_venda"),
            _min("docdate").alias("data_primeira_venda"),
            countDistinct(col("docdate").substr(1, 7)).alias("meses_ativos"),
        )
    )

    # Clientes ativos vs inativos (últimos 90 dias)
    clientes_ativos = (
        invoices
        .filter((col("cancelled") != "tYES") & (datediff(current_date(), col("docdate")) <= 90))
        .groupBy("salespersoncode")
        .agg(countDistinct("cardcode").alias("clientes_ativos_90d"))
    )

    clientes_inativos = (
        invoices
        .filter((col("cancelled") != "tYES") & (datediff(current_date(), col("docdate")) > 90))
        .groupBy("salespersoncode")
        .agg(countDistinct("cardcode").alias("clientes_inativos_90d"))
    )

    df = (
        perf
        .join(clientes_ativos, on="salespersoncode", how="left")
        .join(clientes_inativos, on="salespersoncode", how="left")
        .join(
            vendedores.select(
                col("salesemployeecode").alias("salespersoncode"),
                col("salesemployeename"),
                col("commissionforsalesemployee").alias("pct_comissao"),
                col("active").alias("vendedor_ativo"),
            ),
            on="salespersoncode", how="left"
        )
        .withColumn("clientes_ativos_90d", when(col("clientes_ativos_90d").isNull(), lit(0)).otherwise(col("clientes_ativos_90d")))
        .withColumn("clientes_inativos_90d", when(col("clientes_inativos_90d").isNull(), lit(0)).otherwise(col("clientes_inativos_90d")))
        .withColumn(
            "pct_carteira_ativa",
            when(col("total_clientes") > 0,
                 _round(col("clientes_ativos_90d") / col("total_clientes") * 100, 2))
            .otherwise(lit(0.0))
        )
        .withColumn(
            "pct_margem",
            when(col("receita_total") > 0,
                 _round(col("lucro_bruto_total") / col("receita_total") * 100, 2))
            .otherwise(lit(0.0))
        )
        .withColumn(
            "receita_por_cliente",
            when(col("total_clientes") > 0,
                 _round(col("receita_total") / col("total_clientes"), 2))
            .otherwise(lit(0.0))
        )
        .withColumnRenamed("salespersoncode", "cod_vendedor")
        .withColumnRenamed("salesemployeename", "nome_vendedor")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    write_gold(spark, df, "cobertura_vendedor")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    spark = get_spark_session(args["JOB_NAME"])

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{GOLD_DATABASES['cadastros']['database']}"
    )

    print("\n" + "=" * 60)
    print("  GOLD — Dominio Cadastros (3 tabelas)")
    print("=" * 60)

    # Carrega fontes uma vez em cache (column pruning)
    print("\n[LOAD] Carregando Silver em cache...")
    invoices = (
        spark.table(silver_table("invoices"))
        .select("cardcode", "cardname", "docentry", "docdate", "docnum",
                "linetotal", "grossprofit", "quantity", "unitprice",
                "salespersoncode", "itemcode", "itemdescription",
                "warehousecode", "cancelled", "linediscountpercent")
        .cache()
    )
    invoices.count()  # materializa

    bp = (
        spark.table(silver_table("business_partners"))
        .filter(col("_is_current") == True)
        .select("cardcode", "cardtype", "groupcode", "creditlimit",
                "currentaccountbalance", "city", "billtostate")
        .cache()
    )

    items = (
        spark.table(silver_table("items"))
        .filter(col("_is_current") == True)
        .cache()
    )

    item_groups = spark.table(silver_table("item_groups")).cache()

    vendedores = (
        spark.table(silver_table("sales_persons"))
        .filter(col("_is_current") == True)
        .cache()
    )

    print("[LOAD] Cache pronto.")

    # Gera as 3 tabelas
    build_perfil_cliente(spark, invoices, bp)
    build_catalogo_produtos(spark, invoices, items, item_groups)
    build_cobertura_vendedor(spark, invoices, vendedores)

    # Libera
    invoices.unpersist()
    bp.unpersist()
    items.unpersist()
    item_groups.unpersist()
    vendedores.unpersist()

    spark.stop()
    print("\n[COMPLETE] Job Gold Cadastros finalizado.")


if __name__ == "__main__":
    main()
