"""
AWS Glue Job: Bronze → Silver — Dimensões (SCD2)
Entidades: BusinessPartners, Items, ItemGroups, SalesPersons

Schema baseado na saída real do SAP B1 Service Layer (JSON arrays).
Execução: Glue 4.0, G.1X, 2 workers com auto-scaling.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, to_date, lit

from config import ENTITIES, CATALOG_DATABASE_SILVER
from utils import (
    get_spark_session, read_bronze_json, deduplicate, add_silver_metadata,
    generate_surrogate_key, mask_pii, clean_strings, validate_not_null_keys
)
from scd2 import apply_scd2
from iceberg_writer import write_iceberg_merge


def transform_business_partners(spark, cfg):
    """
    BusinessPartners (OCRD via Service Layer).
    Campos reais: CardCode, CardName, CardType, GroupCode, Phone1, Phone2,
    Cellular, EmailAddress, FederalTaxID, City, BillToState, SalesPersonCode, etc.
    """
    df = read_bronze_json(spark, cfg["bronze_path"])

    df = df.select(
        col("CardCode").cast("string"),
        col("CardName").cast("string"),
        col("CardType").cast("string"),
        col("GroupCode").cast("int"),
        col("Phone1").cast("string"),
        col("Phone2").cast("string"),
        col("Fax").cast("string"),
        col("Cellular").cast("string"),
        col("EmailAddress").cast("string"),
        col("FederalTaxID").cast("string"),
        col("Address").cast("string"),
        col("ZipCode").cast("string"),
        col("City").cast("string"),
        col("County").cast("string"),
        col("Country").cast("string"),
        col("BillToState").cast("string"),
        col("ShipToState").cast("string"),
        col("SalesPersonCode").cast("int"),
        col("Currency").cast("string"),
        col("PayTermsGrpCode").cast("int"),
        col("PriceListNum").cast("int"),
        col("DiscountPercent").cast("double"),
        col("CreditLimit").cast("double"),
        col("MaxCommitment").cast("double"),
        col("CurrentAccountBalance").cast("double"),
        col("OpenDeliveryNotesBalance").cast("double"),
        col("OpenOrdersBalance").cast("double"),
        col("CardForeignName").cast("string"),
        col("Valid").cast("string"),
        col("ValidFrom").cast("string"),
        col("ValidTo").cast("string"),
        col("Frozen").cast("string"),
        col("FrozenFrom").cast("string"),
        col("FrozenTo").cast("string"),
        col("Block").cast("string"),
        col("CreationDate").cast("string"),
        col("UpdateDate").cast("string"),
        col("_source_file"),
    )

    # Limpeza
    df = clean_strings(df)
    df = validate_not_null_keys(df, cfg["primary_key"])
    df = deduplicate(df, cfg["primary_key"])

    # Tipagem de datas
    df = (
        df
        .withColumn("CreationDate", to_date(col("CreationDate")))
        .withColumn("UpdateDate", to_date(col("UpdateDate")))
        .withColumn("ValidFrom", to_date(col("ValidFrom")))
        .withColumn("ValidTo", to_date(col("ValidTo")))
        .withColumn("FrozenFrom", to_date(col("FrozenFrom")))
        .withColumn("FrozenTo", to_date(col("FrozenTo")))
    )

    # Mascaramento PII (LGPD)
    df = mask_pii(df, cfg["pii_columns"])

    # Metadata
    df = add_silver_metadata(df)
    df = generate_surrogate_key(df, cfg["primary_key"], "_sk")

    # Remove coluna auxiliar
    df = df.drop("_source_file")

    return df


def transform_items(spark, cfg):
    """
    Items (OITM via Service Layer).
    Campos: ItemCode, ItemName, ForeignName, ItemsGroupCode, BarCode,
    QuantityOnStock, Valid, SalesUnit, PurchaseUnit, etc.
    """
    df = read_bronze_json(spark, cfg["bronze_path"])

    df = df.select(
        col("ItemCode").cast("string"),
        col("ItemName").cast("string"),
        col("ForeignName").cast("string"),
        col("ItemsGroupCode").cast("int"),
        col("BarCode").cast("string"),
        col("VatLiable").cast("string"),
        col("PurchaseItem").cast("string"),
        col("SalesItem").cast("string"),
        col("InventoryItem").cast("string"),
        col("Mainsupplier").cast("string"),
        col("DesiredInventory").cast("double"),
        col("MinInventory").cast("double"),
        col("QuantityOnStock").cast("double"),
        col("QuantityOrderedFromVendors").cast("double"),
        col("QuantityOrderedByCustomers").cast("double"),
        col("Valid").cast("string"),
        col("ValidFrom").cast("string"),
        col("ValidTo").cast("string"),
        col("Frozen").cast("string"),
        col("FrozenFrom").cast("string"),
        col("FrozenTo").cast("string"),
        col("SalesUnit").cast("string"),
        col("PurchaseUnit").cast("string"),
        col("Manufacturer").cast("int"),
        col("CommissionPercent").cast("double"),
        col("CommissionGroup").cast("int"),
        col("TreeType").cast("string"),
        col("AssetItem").cast("string"),
        col("_source_file"),
    )

    df = clean_strings(df)
    df = validate_not_null_keys(df, cfg["primary_key"])
    df = deduplicate(df, cfg["primary_key"])

    df = (
        df
        .withColumn("ValidFrom", to_date(col("ValidFrom")))
        .withColumn("ValidTo", to_date(col("ValidTo")))
        .withColumn("FrozenFrom", to_date(col("FrozenFrom")))
        .withColumn("FrozenTo", to_date(col("FrozenTo")))
    )

    df = add_silver_metadata(df)
    df = generate_surrogate_key(df, cfg["primary_key"], "_sk")
    df = df.drop("_source_file")

    return df


def transform_item_groups(spark, cfg):
    """
    ItemGroups (OITB via Service Layer).
    Campos relevantes: Number, GroupName, campos contábeis (contas).
    """
    df = read_bronze_json(spark, cfg["bronze_path"])

    df = df.select(
        col("Number").cast("int"),
        col("GroupName").cast("string"),
        col("ProcurementMethod").cast("string"),
        col("InventorySystem").cast("string"),
        col("PlanningSystem").cast("string"),
        col("ItemClass").cast("string"),
        col("Alert").cast("string"),
        col("RawMaterial").cast("string"),
        col("_source_file"),
    )

    df = clean_strings(df)
    df = validate_not_null_keys(df, cfg["primary_key"])
    df = deduplicate(df, cfg["primary_key"])
    df = add_silver_metadata(df)
    df = generate_surrogate_key(df, cfg["primary_key"], "_sk")
    df = df.drop("_source_file")

    return df


def transform_sales_persons(spark, cfg):
    """
    SalesPersons (OSLP via Service Layer).
    Campos: SalesEmployeeCode, SalesEmployeeName, Telephone, Mobile, Email, etc.
    """
    df = read_bronze_json(spark, cfg["bronze_path"])

    df = df.select(
        col("SalesEmployeeCode").cast("int"),
        col("SalesEmployeeName").cast("string"),
        col("Remarks").cast("string"),
        col("CommissionForSalesEmployee").cast("double"),
        col("CommissionGroup").cast("int"),
        col("Locked").cast("string"),
        col("EmployeeID").cast("int"),
        col("Active").cast("string"),
        col("Telephone").cast("string"),
        col("Mobile").cast("string"),
        col("Fax").cast("string"),
        col("Email").cast("string"),
        col("_source_file"),
    )

    df = clean_strings(df)
    df = validate_not_null_keys(df, cfg["primary_key"])
    df = deduplicate(df, cfg["primary_key"])
    df = mask_pii(df, cfg["pii_columns"])
    df = add_silver_metadata(df)
    df = generate_surrogate_key(df, cfg["primary_key"], "_sk")
    df = df.drop("_source_file")

    return df


# Mapeamento entidade → função
TRANSFORM_MAP = {
    "BusinessPartners": transform_business_partners,
    "Items": transform_items,
    "ItemGroups": transform_item_groups,
    "SalesPersons": transform_sales_persons,
}


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    # Parâmetro opcional: entidades a processar (default: todas)
    try:
        resolved = getResolvedOptions(sys.argv, ["entities"])
        entities_to_process = resolved["entities"].split(",")
    except Exception:
        entities_to_process = list(TRANSFORM_MAP.keys())

    spark = get_spark_session(args["JOB_NAME"])

    # Garante database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{CATALOG_DATABASE_SILVER}")

    for entity_name in entities_to_process:
        entity_name = entity_name.strip()
        if entity_name not in TRANSFORM_MAP:
            print(f"[SKIP] Entidade desconhecida: {entity_name}")
            continue

        print(f"\n{'='*60}")
        print(f"[START] Dimensão: {entity_name}")
        print(f"{'='*60}")

        cfg = ENTITIES[entity_name]
        transform_fn = TRANSFORM_MAP[entity_name]

        try:
            df = transform_fn(spark, cfg)
            record_count = df.count()
            print(f"[INFO] {record_count} registros após transformação")

            if cfg["scd_type"] == 2:
                apply_scd2(spark, df, cfg["silver_table"], cfg["primary_key"])
            else:
                write_iceberg_merge(spark, df, cfg["silver_table"], cfg["primary_key"])

            print(f"[DONE] {entity_name} → {CATALOG_DATABASE_SILVER}.{cfg['silver_table']} ✓")

        except Exception as e:
            print(f"[ERROR] Falha em {entity_name}: {str(e)}")
            raise

    spark.stop()
    print("\n[COMPLETE] Job de dimensões finalizado com sucesso.")


if __name__ == "__main__":
    main()
