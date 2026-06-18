"""
Data Quality Expectations — inspirado em Great Expectations.
Validações declarativas executadas via PySpark (sem dependências externas).

Uso: chamado após transformação, antes da escrita Iceberg.
Se threshold não for atingido, bloqueia escrita e loga falha.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, lit


# ═══════════════════════════════════════════════════════════════
# Expectations por entidade (configuração declarativa)
# ═══════════════════════════════════════════════════════════════

EXPECTATIONS = {
    "business_partners": {
        "expect_column_values_to_not_be_null": ["CardCode", "CardName"],
        "expect_column_values_to_be_unique": ["CardCode"],
        "expect_column_values_to_be_in_set": {
            "CardType": ["cCustomer", "cSupplier", "cLead"],
        },
        "expect_column_values_to_be_between": {
            "CreditLimit": {"min": 0, "max": 10000000},
        },
        "threshold_pct": 99.0,
    },
    "items": {
        "expect_column_values_to_not_be_null": ["ItemCode", "ItemName"],
        "expect_column_values_to_be_unique": ["ItemCode"],
        "threshold_pct": 99.0,
    },
    "sales_persons": {
        "expect_column_values_to_not_be_null": ["SalesEmployeeCode", "SalesEmployeeName"],
        "expect_column_values_to_be_unique": ["SalesEmployeeCode"],
        "threshold_pct": 99.0,
    },
    "item_groups": {
        "expect_column_values_to_not_be_null": ["Number", "GroupName"],
        "expect_column_values_to_be_unique": ["Number"],
        "threshold_pct": 99.0,
    },
    "invoices": {
        "expect_column_values_to_not_be_null": ["DocEntry", "LineNum", "DocDate", "CardCode"],
        "expect_compound_columns_to_be_unique": ["DocEntry", "LineNum"],
        "expect_column_values_to_be_between": {
            "Quantity": {"min": 0, "max": 1000000},
            "LineTotal": {"min": -1000000, "max": 100000000},
        },
        "threshold_pct": 99.0,
    },
    "orders": {
        "expect_column_values_to_not_be_null": ["DocEntry", "LineNum", "DocDate", "CardCode"],
        "expect_compound_columns_to_be_unique": ["DocEntry", "LineNum"],
        "threshold_pct": 99.0,
    },
    "quotations": {
        "expect_column_values_to_not_be_null": ["DocEntry", "LineNum", "DocDate", "CardCode"],
        "expect_compound_columns_to_be_unique": ["DocEntry", "LineNum"],
        "expect_column_values_to_be_in_set": {
            "DocumentStatus": ["bost_Open", "bost_Close"],
        },
        "threshold_pct": 99.0,
    },
    "inventory_gen_entries": {
        "expect_column_values_to_not_be_null": ["DocEntry", "LineNum"],
        "expect_compound_columns_to_be_unique": ["DocEntry", "LineNum"],
        "threshold_pct": 99.0,
    },
}


# ═══════════════════════════════════════════════════════════════
# Engine de validação
# ═══════════════════════════════════════════════════════════════

def validate(df: DataFrame, table_name: str) -> dict:
    """
    Executa expectations para uma tabela. Retorna relatório.
    Não falha o job — apenas reporta. O caller decide se bloqueia.
    """
    if table_name not in EXPECTATIONS:
        print(f"[QUALITY] Sem expectations definidas para {table_name}")
        return {"table": table_name, "status": "NO_EXPECTATIONS"}

    exp = EXPECTATIONS[table_name]
    threshold = exp.get("threshold_pct", 99.0)
    total = df.count()
    results = []

    if total == 0:
        return {"table": table_name, "status": "EMPTY", "total": 0}

    # --- expect_column_values_to_not_be_null ---
    for col_name in exp.get("expect_column_values_to_not_be_null", []):
        if col_name not in df.columns:
            results.append({"check": "not_null", "column": col_name, "status": "COLUMN_MISSING"})
            continue
        null_count = df.filter(col(col_name).isNull()).count()
        pct_valid = ((total - null_count) / total) * 100
        results.append({
            "check": "not_null",
            "column": col_name,
            "null_count": null_count,
            "pct_valid": round(pct_valid, 2),
            "status": "PASS" if pct_valid >= threshold else "FAIL",
        })

    # --- expect_column_values_to_be_unique ---
    for col_name in exp.get("expect_column_values_to_be_unique", []):
        if col_name not in df.columns:
            continue
        distinct = df.select(col_name).distinct().count()
        duplicates = total - distinct
        results.append({
            "check": "unique",
            "column": col_name,
            "duplicates": duplicates,
            "status": "PASS" if duplicates == 0 else "FAIL",
        })

    # --- expect_compound_columns_to_be_unique ---
    compound_cols = exp.get("expect_compound_columns_to_be_unique", [])
    if compound_cols:
        distinct = df.select(*compound_cols).distinct().count()
        duplicates = total - distinct
        results.append({
            "check": "compound_unique",
            "columns": compound_cols,
            "duplicates": duplicates,
            "status": "PASS" if duplicates == 0 else "FAIL",
        })

    # --- expect_column_values_to_be_in_set ---
    for col_name, valid_values in exp.get("expect_column_values_to_be_in_set", {}).items():
        if col_name not in df.columns:
            continue
        invalid = df.filter(
            ~col(col_name).isin(valid_values) & col(col_name).isNotNull()
        ).count()
        pct_valid = ((total - invalid) / total) * 100
        results.append({
            "check": "in_set",
            "column": col_name,
            "invalid_count": invalid,
            "valid_values": valid_values,
            "pct_valid": round(pct_valid, 2),
            "status": "PASS" if pct_valid >= threshold else "WARN",
        })

    # --- expect_column_values_to_be_between ---
    for col_name, bounds in exp.get("expect_column_values_to_be_between", {}).items():
        if col_name not in df.columns:
            continue
        out_of_range = df.filter(
            (col(col_name) < bounds["min"]) | (col(col_name) > bounds["max"])
        ).count()
        pct_valid = ((total - out_of_range) / total) * 100
        results.append({
            "check": "between",
            "column": col_name,
            "out_of_range": out_of_range,
            "bounds": bounds,
            "pct_valid": round(pct_valid, 2),
            "status": "PASS" if pct_valid >= threshold else "WARN",
        })

    # --- Relatório ---
    failures = sum(1 for r in results if r["status"] == "FAIL")
    warnings = sum(1 for r in results if r["status"] == "WARN")
    passed = sum(1 for r in results if r["status"] == "PASS")

    print(f"\n  [QUALITY] {table_name}: {len(results)} checks | "
          f"Pass: {passed} | Warn: {warnings} | Fail: {failures}")
    for r in results:
        icon = "✓" if r["status"] == "PASS" else "✗" if r["status"] == "FAIL" else "⚠"
        print(f"    [{icon}] {r['check']:20s} | {r.get('column', r.get('columns', ''))}")

    return {
        "table": table_name,
        "total": total,
        "checks": len(results),
        "passed": passed,
        "warnings": warnings,
        "failures": failures,
        "status": "PASS" if failures == 0 else "FAIL",
        "details": results,
    }


def validate_or_fail(df: DataFrame, table_name: str) -> DataFrame:
    """
    Valida e FALHA (raise) se threshold não for atingido.
    Usar entre transformação e escrita para bloquear dados ruins.
    """
    report = validate(df, table_name)
    if report.get("status") == "FAIL":
        raise Exception(
            f"QUALITY GATE FAILED para {table_name}: "
            f"{report['failures']} check(s) falharam. Escrita bloqueada."
        )
    return df
