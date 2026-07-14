"""
AWS Glue Job: Bronze → Silver — Fatos (Transacionais)
Entidades: Invoices, Orders, Quotations, InventoryGenEntries

Schema real do SAP B1 Service Layer:
- Documentos com header (DocEntry, DocNum, CardCode, DocDate, DocTotal, etc.)
- DocumentLines[] aninhado (LineNum, ItemCode, Quantity, Price, LineTotal, etc.)
- Desnormaliza header + lines para tabela flat (star schema ready)

Execução: Glue 4.0, G.1X, 2 workers com auto-scaling.
Volumes: Invoices ~8GB, Orders ~10GB (full load) — auto-scaling escala workers.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, to_date, explode, coalesce, lit, expr
from pyspark.sql.types import ArrayType, StructType

from config import ENTITIES, CATALOG_DATABASE_SILVER
from utils import (
    get_spark_session, read_bronze_json, deduplicate, add_silver_metadata,
    clean_strings, validate_not_null_keys, add_date_partitions
)
from iceberg_writer import write_iceberg_merge


def _extract_additional_expenses(df):
    """
    Calcula o total de despesas adicionais do cabeçalho da NF
    (DocumentAdditionalExpenses[].LineTotal — frete e outras despesas
    cobradas separadamente das linhas de item).

    Necessário para reconciliação fiscal:
    DocTotal ≈ SUM(LineTotal) + VatSum + DocumentAdditionalExpenses

    Sem esse campo, ~5-8% das NFs (as que têm frete no cabeçalho) apareciam
    como divergência na reconciliação Silver, mesmo estando corretas.
    """
    if "DocumentAdditionalExpenses" not in df.columns:
        return df.withColumn("DocumentAdditionalExpenses", lit(0.0))

    field_type = df.schema["DocumentAdditionalExpenses"].dataType
    has_linetotal = (
        isinstance(field_type, ArrayType)
        and isinstance(field_type.elementType, StructType)
        and "LineTotal" in field_type.elementType.fieldNames()
    )
    if not has_linetotal:
        # Schema inferido sem despesas (ex.: todos os docs do arquivo sem esse campo)
        return df.withColumn("DocumentAdditionalExpenses", lit(0.0))

    return df.withColumn(
        "DocumentAdditionalExpenses",
        expr(
            "coalesce(aggregate(DocumentAdditionalExpenses, cast(0.0 as double), "
            "(acc, x) -> acc + coalesce(x.LineTotal, cast(0.0 as double))), cast(0.0 as double))"
        )
    )


def transform_invoices(spark, cfg, load_type="all", load_date=None):
    """
    Invoices (OINV + INV1 via Service Layer).
    Desnormaliza DocumentLines — cada linha vira um registro.
    PK final: DocEntry + LineNum
    """
    df = read_bronze_json(spark, cfg["bronze_path"], load_type, load_date)

    # Despesas adicionais do cabeçalho (frete) — calculado antes do explode
    # das linhas, pois é um valor agregado do documento (não por linha).
    df = _extract_additional_expenses(df)

    # Explode DocumentLines
    df_exploded = df.select(
        # Header
        col("DocEntry").cast("int"),
        col("DocNum").cast("int"),
        col("DocType").cast("string"),
        col("DocDate").cast("string"),
        col("DocDueDate").cast("string"),
        col("TaxDate").cast("string"),
        col("CardCode").cast("string"),
        col("CardName").cast("string"),
        col("NumAtCard").cast("string"),
        col("DocTotal").cast("double"),
        col("VatSum").cast("double"),
        col("DiscountPercent").cast("double"),
        col("DocCurrency").cast("string"),
        col("DocRate").cast("double"),
        col("SalesPersonCode").cast("int"),
        col("DocumentStatus").cast("string"),
        col("Cancelled").cast("string"),
        col("PaymentGroupCode").cast("int"),
        col("TransportationCode").cast("int"),
        col("Series").cast("int"),
        col("CreationDate").cast("string"),
        col("UpdateDate").cast("string"),
        col("Comments").cast("string"),
        col("Reference1").cast("string"),
        col("BPL_IDAssignedToInvoice").cast("int").alias("BranchId"),
        col("BPLName").cast("string").alias("NomeFilial"),
        col("NumberOfInstallments").cast("int"),
        col("DocumentAdditionalExpenses").cast("double"),
        col("_source_file"),
        # Lines
        explode(col("DocumentLines")).alias("line"),
    )

    # Flatten line fields
    result = df_exploded.select(
        # Header
        "DocEntry", "DocNum", "DocType", "DocDate", "DocDueDate", "TaxDate",
        "CardCode", "CardName", "NumAtCard", "DocTotal", "VatSum",
        "DiscountPercent", "DocCurrency", "DocRate", "SalesPersonCode",
        "DocumentStatus", "Cancelled", "PaymentGroupCode",
        "TransportationCode", "Series", "CreationDate", "UpdateDate",
        "Comments", "Reference1", "BranchId", "NomeFilial", "NumberOfInstallments",
        "DocumentAdditionalExpenses", "_source_file",
        # Line
        col("line.LineNum").cast("int").alias("LineNum"),
        col("line.ItemCode").cast("string").alias("ItemCode"),
        col("line.ItemDescription").cast("string").alias("ItemDescription"),
        col("line.Quantity").cast("double").alias("Quantity"),
        col("line.Price").cast("double").alias("UnitPrice"),
        col("line.PriceAfterVAT").cast("double").alias("PriceAfterVAT"),
        col("line.LineTotal").cast("double").alias("LineTotal"),
        col("line.DiscountPercent").cast("double").alias("LineDiscountPercent"),
        col("line.WarehouseCode").cast("string").alias("WarehouseCode"),
        col("line.SalesPersonCode").cast("int").alias("LineSalesPersonCode"),
        col("line.CostingCode").cast("string").alias("CostCenter"),
        col("line.AccountCode").cast("string").alias("AccountCode"),
        col("line.ShipDate").cast("string").alias("LineShipDate"),
        col("line.Currency").cast("string").alias("LineCurrency"),
        col("line.BarCode").cast("string").alias("BarCode"),
        col("line.GrossProfit").cast("double").alias("GrossProfit"),
        col("line.CFOPCode").cast("string").alias("CFOPCode"),
        col("line.BaseEntry").cast("int").alias("BaseEntry"),
        col("line.BaseType").cast("int").alias("BaseType"),
        col("line.LineStatus").cast("string").alias("LineStatus"),
        col("line.FreeOfChargeBP").cast("string").alias("FreeOfChargeBP"),
        col("line.GrossBuyPrice").cast("double").alias("GrossBuyPrice"),
        col("line.GrossTotal").cast("double").alias("ValorTotalLinha"),
        col("line.ActualDeliveryDate").cast("string").alias("ActualDeliveryDate"),
    )

    # Limpeza e dedup
    result = clean_strings(result)
    result = validate_not_null_keys(result, cfg["primary_key"])
    result = deduplicate(result, cfg["primary_key"])

    # Tipagem de datas
    result = (
        result
        .withColumn("DocDate", to_date(col("DocDate")))
        .withColumn("DocDueDate", to_date(col("DocDueDate")))
        .withColumn("TaxDate", to_date(col("TaxDate")))
        .withColumn("CreationDate", to_date(col("CreationDate")))
        .withColumn("UpdateDate", to_date(col("UpdateDate")))
        .withColumn("LineShipDate", to_date(col("LineShipDate")))
    )

    # Particionamento por DocDate
    result = add_date_partitions(result, "DocDate")

    # Metadata
    result = add_silver_metadata(result)
    result = result.drop("_source_file")

    return result


def transform_orders(spark, cfg, load_type="all", load_date=None):
    """
    Orders (ORDR + RDR1 via Service Layer).
    Mesma estrutura de Invoices (DocumentLines aninhado).
    """
    df = read_bronze_json(spark, cfg["bronze_path"], load_type, load_date)
    if df is None:
        return None

    df_exploded = df.select(
        col("DocEntry").cast("int"),
        col("DocNum").cast("int"),
        col("DocType").cast("string"),
        col("DocDate").cast("string"),
        col("DocDueDate").cast("string"),
        col("TaxDate").cast("string"),
        col("CardCode").cast("string"),
        col("CardName").cast("string"),
        col("NumAtCard").cast("string"),
        col("DocTotal").cast("double"),
        col("VatSum").cast("double"),
        col("DiscountPercent").cast("double"),
        col("DocCurrency").cast("string"),
        col("DocRate").cast("double"),
        col("SalesPersonCode").cast("int"),
        col("DocumentStatus").cast("string"),
        col("Cancelled").cast("string"),
        col("PaymentGroupCode").cast("int"),
        col("TransportationCode").cast("int"),
        col("Series").cast("int"),
        col("CreationDate").cast("string"),
        col("UpdateDate").cast("string"),
        col("Comments").cast("string"),
        col("Reference1").cast("string"),
        col("BPL_IDAssignedToInvoice").cast("int").alias("BranchId"),
        col("NumberOfInstallments").cast("int"),
        col("_source_file"),
        explode(col("DocumentLines")).alias("line"),
    )

    result = df_exploded.select(
        "DocEntry", "DocNum", "DocType", "DocDate", "DocDueDate", "TaxDate",
        "CardCode", "CardName", "NumAtCard", "DocTotal", "VatSum",
        "DiscountPercent", "DocCurrency", "DocRate", "SalesPersonCode",
        "DocumentStatus", "Cancelled", "PaymentGroupCode",
        "TransportationCode", "Series", "CreationDate", "UpdateDate",
        "Comments", "Reference1", "BranchId", "NumberOfInstallments", "_source_file",
        col("line.LineNum").cast("int").alias("LineNum"),
        col("line.ItemCode").cast("string").alias("ItemCode"),
        col("line.ItemDescription").cast("string").alias("ItemDescription"),
        col("line.Quantity").cast("double").alias("Quantity"),
        col("line.Price").cast("double").alias("UnitPrice"),
        col("line.PriceAfterVAT").cast("double").alias("PriceAfterVAT"),
        col("line.LineTotal").cast("double").alias("LineTotal"),
        col("line.DiscountPercent").cast("double").alias("LineDiscountPercent"),
        col("line.WarehouseCode").cast("string").alias("WarehouseCode"),
        col("line.SalesPersonCode").cast("int").alias("LineSalesPersonCode"),
        col("line.CostingCode").cast("string").alias("CostCenter"),
        col("line.AccountCode").cast("string").alias("AccountCode"),
        col("line.ShipDate").cast("string").alias("LineShipDate"),
        col("line.Currency").cast("string").alias("LineCurrency"),
        col("line.BarCode").cast("string").alias("BarCode"),
        col("line.GrossProfit").cast("double").alias("GrossProfit"),
        col("line.CFOPCode").cast("string").alias("CFOPCode"),
        col("line.BaseEntry").cast("int").alias("BaseEntry"),
        col("line.BaseType").cast("int").alias("BaseType"),
        col("line.LineStatus").cast("string").alias("LineStatus"),
        col("line.FreeOfChargeBP").cast("string").alias("FreeOfChargeBP"),
        col("line.GrossBuyPrice").cast("double").alias("GrossBuyPrice"),
        col("line.ActualDeliveryDate").cast("string").alias("ActualDeliveryDate"),
    )

    result = clean_strings(result)
    result = validate_not_null_keys(result, cfg["primary_key"])
    result = deduplicate(result, cfg["primary_key"])

    result = (
        result
        .withColumn("DocDate", to_date(col("DocDate")))
        .withColumn("DocDueDate", to_date(col("DocDueDate")))
        .withColumn("TaxDate", to_date(col("TaxDate")))
        .withColumn("CreationDate", to_date(col("CreationDate")))
        .withColumn("UpdateDate", to_date(col("UpdateDate")))
        .withColumn("LineShipDate", to_date(col("LineShipDate")))
    )

    result = add_date_partitions(result, "DocDate")
    result = add_silver_metadata(result)
    result = result.drop("_source_file")

    return result


def transform_quotations(spark, cfg, load_type="all", load_date=None):
    """
    Quotations (OQUT + QUT1 via Service Layer).
    Base para CU-02: win-rate, análise preditiva.
    DocumentStatus: 'bost_Open' / 'bost_Close' — importante para conversão.
    """
    df = read_bronze_json(spark, cfg["bronze_path"], load_type, load_date)
    if df is None:
        return None

    df_exploded = df.select(
        col("DocEntry").cast("int"),
        col("DocNum").cast("int"),
        col("DocType").cast("string"),
        col("DocDate").cast("string"),
        col("DocDueDate").cast("string"),
        col("TaxDate").cast("string"),
        col("CardCode").cast("string"),
        col("CardName").cast("string"),
        col("NumAtCard").cast("string"),
        col("DocTotal").cast("double"),
        col("VatSum").cast("double"),
        col("DiscountPercent").cast("double"),
        col("DocCurrency").cast("string"),
        col("SalesPersonCode").cast("int"),
        col("DocumentStatus").cast("string"),
        col("Cancelled").cast("string"),
        col("Series").cast("int"),
        col("CreationDate").cast("string"),
        col("UpdateDate").cast("string"),
        col("Comments").cast("string"),
        col("_source_file"),
        explode(col("DocumentLines")).alias("line"),
    )

    result = df_exploded.select(
        "DocEntry", "DocNum", "DocType", "DocDate", "DocDueDate", "TaxDate",
        "CardCode", "CardName", "NumAtCard", "DocTotal", "VatSum",
        "DiscountPercent", "DocCurrency", "SalesPersonCode",
        "DocumentStatus", "Cancelled", "Series", "CreationDate", "UpdateDate",
        "Comments", "_source_file",
        col("line.LineNum").cast("int").alias("LineNum"),
        col("line.ItemCode").cast("string").alias("ItemCode"),
        col("line.ItemDescription").cast("string").alias("ItemDescription"),
        col("line.Quantity").cast("double").alias("Quantity"),
        col("line.Price").cast("double").alias("UnitPrice"),
        col("line.PriceAfterVAT").cast("double").alias("PriceAfterVAT"),
        col("line.LineTotal").cast("double").alias("LineTotal"),
        col("line.DiscountPercent").cast("double").alias("LineDiscountPercent"),
        col("line.WarehouseCode").cast("string").alias("WarehouseCode"),
        col("line.Currency").cast("string").alias("LineCurrency"),
        col("line.GrossProfit").cast("double").alias("GrossProfit"),
        col("line.CFOPCode").cast("string").alias("CFOPCode"),
        col("line.BaseEntry").cast("int").alias("BaseEntry"),
        col("line.BaseType").cast("int").alias("BaseType"),
        col("line.LineStatus").cast("string").alias("LineStatus"),
        col("line.FreeOfChargeBP").cast("string").alias("FreeOfChargeBP"),
        col("line.GrossBuyPrice").cast("double").alias("GrossBuyPrice"),
        col("line.ActualDeliveryDate").cast("string").alias("ActualDeliveryDate"),
    )

    result = clean_strings(result)
    result = validate_not_null_keys(result, cfg["primary_key"])
    result = deduplicate(result, cfg["primary_key"])

    result = (
        result
        .withColumn("DocDate", to_date(col("DocDate")))
        .withColumn("DocDueDate", to_date(col("DocDueDate")))
        .withColumn("TaxDate", to_date(col("TaxDate")))
        .withColumn("CreationDate", to_date(col("CreationDate")))
        .withColumn("UpdateDate", to_date(col("UpdateDate")))
    )

    result = add_date_partitions(result, "DocDate")
    result = add_silver_metadata(result)
    result = result.drop("_source_file")

    return result


def transform_inventory_gen_entries(spark, cfg, load_type="all", load_date=None):
    """
    InventoryGenEntries (OIGN via Service Layer).
    Entradas de mercadoria em estoque.
    """
    df = read_bronze_json(spark, cfg["bronze_path"], load_type, load_date)
    if df is None:
        return None

    df_exploded = df.select(
        col("DocEntry").cast("int"),
        col("DocNum").cast("int"),
        col("DocType").cast("string"),
        col("DocDate").cast("string"),
        col("DocDueDate").cast("string"),
        col("DocTotal").cast("double"),
        col("DocCurrency").cast("string"),
        col("Comments").cast("string"),
        col("JournalMemo").cast("string"),
        col("CreationDate").cast("string"),
        col("UpdateDate").cast("string"),
        col("_source_file"),
        explode(col("DocumentLines")).alias("line"),
    )

    result = df_exploded.select(
        "DocEntry", "DocNum", "DocType", "DocDate", "DocDueDate",
        "DocTotal", "DocCurrency", "Comments", "JournalMemo",
        "CreationDate", "UpdateDate", "_source_file",
        col("line.LineNum").cast("int").alias("LineNum"),
        col("line.ItemCode").cast("string").alias("ItemCode"),
        col("line.ItemDescription").cast("string").alias("ItemDescription"),
        col("line.Quantity").cast("double").alias("Quantity"),
        col("line.Price").cast("double").alias("UnitPrice"),
        col("line.LineTotal").cast("double").alias("LineTotal"),
        col("line.WarehouseCode").cast("string").alias("WarehouseCode"),
        col("line.AccountCode").cast("string").alias("AccountCode"),
        col("line.CostingCode").cast("string").alias("CostCenter"),
        col("line.GrossProfit").cast("double").alias("GrossProfit"),
        col("line.CFOPCode").cast("string").alias("CFOPCode"),
        col("line.BaseEntry").cast("int").alias("BaseEntry"),
        col("line.BaseType").cast("int").alias("BaseType"),
        col("line.LineStatus").cast("string").alias("LineStatus"),
        col("line.FreeOfChargeBP").cast("string").alias("FreeOfChargeBP"),
        col("line.GrossBuyPrice").cast("double").alias("GrossBuyPrice"),
        col("line.ActualDeliveryDate").cast("string").alias("ActualDeliveryDate"),
    )

    result = clean_strings(result)
    result = validate_not_null_keys(result, cfg["primary_key"])
    result = deduplicate(result, cfg["primary_key"])

    result = (
        result
        .withColumn("DocDate", to_date(col("DocDate")))
        .withColumn("DocDueDate", to_date(col("DocDueDate")))
        .withColumn("CreationDate", to_date(col("CreationDate")))
        .withColumn("UpdateDate", to_date(col("UpdateDate")))
    )

    result = add_date_partitions(result, "DocDate")
    result = add_silver_metadata(result)
    result = result.drop("_source_file")

    return result


TRANSFORM_MAP = {
    "Invoices": transform_invoices,
    "Orders": transform_orders,
    "Quotations": transform_quotations,
    "InventoryGenEntries": transform_inventory_gen_entries,
}


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    try:
        resolved = getResolvedOptions(sys.argv, ["entities"])
        entities_to_process = resolved["entities"].split(",")
    except Exception:
        entities_to_process = list(TRANSFORM_MAP.keys())

    # Parâmetros de carga: load_type (full|incremental|all) e load_date (YYYY-MM-DD)
    try:
        load_type = getResolvedOptions(sys.argv, ["load_type"])["load_type"]
    except Exception:
        load_type = "all"

    try:
        load_date = getResolvedOptions(sys.argv, ["load_date"])["load_date"]
    except Exception:
        load_date = None

    spark = get_spark_session(args["JOB_NAME"])
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{CATALOG_DATABASE_SILVER}")

    print(f"[CONFIG] load_type={load_type}, load_date={load_date}")

    for entity_name in entities_to_process:
        entity_name = entity_name.strip()
        if entity_name not in TRANSFORM_MAP:
            print(f"[SKIP] Entidade desconhecida: {entity_name}")
            continue

        print(f"\n{'='*60}")
        print(f"[START] Fato: {entity_name}")
        print(f"{'='*60}")

        cfg = ENTITIES[entity_name]
        transform_fn = TRANSFORM_MAP[entity_name]

        try:
            df = transform_fn(spark, cfg, load_type, load_date)
            if df is None:
                print(f"[SKIP] {entity_name}: sem dados incrementais disponíveis.")
                continue
            record_count = df.count()
            print(f"[INFO] {record_count} registros (linhas de documento) após transformação")

            write_iceberg_merge(
                spark, df, cfg["silver_table"],
                primary_key=cfg["primary_key"],
                partition_by=cfg["partition_by"] if cfg["partition_by"] else None
            )

            print(f"[DONE] {entity_name} → {CATALOG_DATABASE_SILVER}.{cfg['silver_table']} ✓")

        except Exception as e:
            print(f"[ERROR] Falha em {entity_name}: {str(e)}")
            raise

    spark.stop()
    print("\n[COMPLETE] Job de fatos finalizado com sucesso.")


if __name__ == "__main__":
    main()
