"""
AWS Glue Job: Quality Checks — Camada Silver
Meta: ≥ 99% de registros aprovados em schema, nulos em chaves e FKs.

Executado após os jobs de dimensões e fatos.
Falha do quality gate gera exceção → visível no CloudWatch / Workflow.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, count, when
from config import ENTITIES, CATALOG_DATABASE_SILVER, ICEBERG_CATALOG
from utils import get_spark_session


QUALITY_THRESHOLD = 99.0  # % mínimo de aprovação


def check_null_keys(spark, table_name: str, primary_key: list) -> dict:
    """Verifica registros com chave primária nula."""
    full_table = f"{ICEBERG_CATALOG}.{CATALOG_DATABASE_SILVER}.{table_name}"
    try:
        df = spark.table(full_table)
    except Exception:
        return {"check": "null_keys", "table": table_name, "status": "TABLE_NOT_FOUND"}

    # Para SCD2, avalia apenas registros correntes
    if "_is_current" in df.columns:
        df = df.filter(col("_is_current") == True)

    total = df.count()
    if total == 0:
        return {"check": "null_keys", "table": table_name, "status": "EMPTY", "total": 0}

    null_condition = None
    for k in primary_key:
        c = col(f"`{k}`").isNull()
        null_condition = c if null_condition is None else (null_condition | c)

    null_count = df.filter(null_condition).count()
    pct_valid = ((total - null_count) / total) * 100

    return {
        "check": "null_keys",
        "table": table_name,
        "total": total,
        "null_count": null_count,
        "pct_valid": round(pct_valid, 2),
        "status": "PASS" if pct_valid >= QUALITY_THRESHOLD else "FAIL",
    }


def check_duplicates(spark, table_name: str, primary_key: list) -> dict:
    """Verifica duplicatas — não deveria haver após MERGE."""
    full_table = f"{ICEBERG_CATALOG}.{CATALOG_DATABASE_SILVER}.{table_name}"
    try:
        df = spark.table(full_table)
    except Exception:
        return {"check": "duplicates", "table": table_name, "status": "TABLE_NOT_FOUND"}

    if "_is_current" in df.columns:
        df = df.filter(col("_is_current") == True)

    total = df.count()
    distinct = df.select(*[col(f"`{k}`") for k in primary_key]).distinct().count()
    duplicates = total - distinct

    return {
        "check": "duplicates",
        "table": table_name,
        "total": total,
        "distinct_keys": distinct,
        "duplicates": duplicates,
        "status": "PASS" if duplicates == 0 else "FAIL",
    }


def check_fk_integrity(spark, fact_table: str, fk_col: str,
                        dim_table: str, pk_col: str) -> dict:
    """Verifica integridade referencial fato → dimensão."""
    fact_full = f"{ICEBERG_CATALOG}.{CATALOG_DATABASE_SILVER}.{fact_table}"
    dim_full = f"{ICEBERG_CATALOG}.{CATALOG_DATABASE_SILVER}.{dim_table}"

    try:
        fact_df = spark.table(fact_full).select(col(f"`{fk_col}`")).filter(col(f"`{fk_col}`").isNotNull()).distinct()
        dim_df = spark.table(dim_full)
        if "_is_current" in dim_df.columns:
            dim_df = dim_df.filter(col("_is_current") == True)
        dim_df = dim_df.select(col(f"`{pk_col}`")).distinct()
    except Exception as e:
        return {"check": "fk_integrity", "fact": fact_table, "dim": dim_table, "status": "ERROR", "error": str(e)}

    total_fks = fact_df.count()
    if total_fks == 0:
        return {"check": "fk_integrity", "fact": fact_table, "dim": dim_table, "status": "EMPTY"}

    orphans = fact_df.join(dim_df, fact_df[fk_col] == dim_df[pk_col], "left_anti")
    orphan_count = orphans.count()
    pct_valid = ((total_fks - orphan_count) / total_fks) * 100

    return {
        "check": "fk_integrity",
        "fact": fact_table,
        "fk": fk_col,
        "dim": dim_table,
        "total_fks": total_fks,
        "orphans": orphan_count,
        "pct_valid": round(pct_valid, 2),
        "status": "PASS" if pct_valid >= QUALITY_THRESHOLD else "WARN",
    }


def run_all_checks(spark) -> list:
    """Executa todas as validações e imprime relatório."""
    results = []

    # 1. Null keys + Duplicatas para todas as entidades
    for entity_name, cfg in ENTITIES.items():
        results.append(check_null_keys(spark, cfg["silver_table"], cfg["primary_key"]))
        results.append(check_duplicates(spark, cfg["silver_table"], cfg["primary_key"]))

    # 2. FK integrity (fatos → dimensões)
    fk_checks = [
        ("invoices", "CardCode", "business_partners", "CardCode"),
        ("invoices", "ItemCode", "items", "ItemCode"),
        ("invoices", "SalesPersonCode", "sales_persons", "SalesEmployeeCode"),
        ("orders", "CardCode", "business_partners", "CardCode"),
        ("orders", "ItemCode", "items", "ItemCode"),
        ("orders", "SalesPersonCode", "sales_persons", "SalesEmployeeCode"),
        ("quotations", "CardCode", "business_partners", "CardCode"),
        ("quotations", "ItemCode", "items", "ItemCode"),
        ("quotations", "SalesPersonCode", "sales_persons", "SalesEmployeeCode"),
    ]

    for fact, fk_col, dim, pk_col in fk_checks:
        results.append(check_fk_integrity(spark, fact, fk_col, dim, pk_col))

    # Relatório
    print("\n" + "=" * 70)
    print("  RELATÓRIO DE QUALIDADE — CAMADA SILVER")
    print("=" * 70)

    failures = 0
    for r in results:
        status = r.get("status", "?")
        icon = "✓" if status == "PASS" else "✗" if status == "FAIL" else "⚠" if status == "WARN" else "?"
        table = r.get("table", r.get("fact", "?"))
        print(f"  [{icon}] {r['check']:20s} | {table:30s} | {status}")
        if status == "FAIL":
            failures += 1
            print(f"       └─ Detalhes: {r}")

    total_checks = len(results)
    passed = sum(1 for r in results if r["status"] == "PASS")
    warned = sum(1 for r in results if r["status"] == "WARN")

    print(f"\n  Total: {total_checks} | Pass: {passed} | Warn: {warned} | Fail: {failures}")
    print("=" * 70)

    if failures > 0:
        raise Exception(
            f"QUALITY GATE FAILED: {failures} check(s) abaixo do threshold ({QUALITY_THRESHOLD}%). "
            f"Ver logs CloudWatch para detalhes."
        )

    return results


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    spark = get_spark_session(args["JOB_NAME"])
    run_all_checks(spark)
    spark.stop()


if __name__ == "__main__":
    main()
