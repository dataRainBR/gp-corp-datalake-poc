"""
AWS Glue Job: Silver → Gold — Extração de Features de Crédito do FreeText
Domínio: cadastros (gpcorp_gold_cadastros)
Tabela: analise_credito

Parseia o campo FreeText dos BusinessPartners (análises de crédito semi-estruturadas)
e gera tabela estruturada para consumo via Amazon Q in QuickSight e agentes.

Dados extraídos via regex:
- Score, risco, probabilidade de inadimplência
- Limites (aprovado, sugerido, em aberto)
- Dados societários (capital, faturamento, tempo CNPJ)
- Restrições e protestos
- Decisão final (prazo/vista/zerado)
- Metadata (analista, data)
"""
import re
import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, DateType
)

from config import (
    CATALOG_DATABASE_SILVER, ICEBERG_CATALOG,
    GOLD_DATABASES, get_gold_table_path
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


# ═══════════════════════════════════════════════════════════════
# Funções de extração (regex)
# ═══════════════════════════════════════════════════════════════

def parse_money(text: str) -> float:
    """Extrai valor monetário de string brasileira."""
    if not text:
        return None
    text = text.replace(".", "").replace(",", ".").strip()
    text = re.sub(r"[^\d.]", "", text)
    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def extract_last_analysis(free_text: str) -> dict:
    """
    Extrai dados da ÚLTIMA reanálise de crédito do FreeText.
    Retorna dict com todas as features extraídas.
    """
    if not free_text or not isinstance(free_text, str):
        return empty_result()

    result = empty_result()

    # --- Score ---
    scores = re.findall(r"SCORE:\s*(Default|\d+)", free_text, re.IGNORECASE)
    if scores:
        last_score = scores[-1]
        result["score_credito"] = None if last_score.lower() == "default" else int(last_score)
        result["score_is_default"] = last_score.lower() == "default"

    # --- Probabilidade de inadimplência ---
    probs = re.findall(r"PROBABILIDADE DE INADIMPL[EÊ]NCIA:\s*(Default|[\d,]+%?)", free_text, re.IGNORECASE)
    if probs:
        last = probs[-1]
        if last.lower() != "default":
            val = last.replace("%", "").replace(",", ".")
            try:
                result["prob_inadimplencia_pct"] = float(val)
            except ValueError:
                pass

    # --- Risco de crédito ---
    riscos = re.findall(r"RISCO DE CR[EÉ]DITO:\s*([\w\s]+?)(?:\n|$)", free_text, re.IGNORECASE)
    if riscos:
        result["risco_credito"] = riscos[-1].strip().lower()

    # --- Prática do mercado ---
    praticas = re.findall(r"PR[AÁ]TICA DO MERCADO:\s*([\w\sà]+?)(?:\n|$)", free_text, re.IGNORECASE)
    if praticas:
        pratica = praticas[-1].strip().lower()
        result["pratica_mercado"] = pratica
        result["apenas_vista"] = "vista" in pratica

    # --- Limite de crédito PJ sugerido ---
    limites_pj = re.findall(r"LIMITE DE CR[EÉ]DITO PESSOA JUR[IÍ]DICA:\s*R\$\s*([\d.,]+)", free_text, re.IGNORECASE)
    if limites_pj:
        result["limite_sugerido_pj"] = parse_money(limites_pj[-1])

    # --- Limite aprovado ---
    limites = re.findall(r"•\s*LIMITE:\s*R\$\s*([\d.,]+)", free_text)
    if limites:
        result["limite_aprovado"] = parse_money(limites[-1])

    # --- Em aberto ---
    abertos = re.findall(r"EM ABERTO:\s*R\$\s*([\d.,]+)", free_text, re.IGNORECASE)
    if abertos:
        result["valor_em_aberto"] = parse_money(abertos[-1])

    # --- Tempo de CNPJ ---
    tempos = re.findall(r"com\s+(\d+)\s+anos?\s+no\s+mercado", free_text, re.IGNORECASE)
    if tempos:
        result["tempo_cnpj_anos"] = int(tempos[-1])

    # --- Data fundação ---
    fundacoes = re.findall(r"Empresa Fundada em\s+(\d{2}/\d{2}/\d{4})", free_text, re.IGNORECASE)
    if fundacoes:
        result["data_fundacao"] = fundacoes[-1]

    # --- Capital social ---
    capitais = re.findall(r"CAPITAL SOCIAL:\s*R\$\s*([\d.,]+)", free_text, re.IGNORECASE)
    if capitais:
        result["capital_social"] = parse_money(capitais[-1])

    # --- Faturamento presumido ---
    fats = re.findall(r"FATURAMENTO PRESUMIDO:\s*R\$\s*([\d.,]+)", free_text, re.IGNORECASE)
    if fats:
        result["faturamento_presumido"] = parse_money(fats[-1])

    # --- CND ---
    cnds = re.findall(r"CND:\s*([\w\sÃÉÊÁ]+?)(?:\n|•|$)", free_text, re.IGNORECASE)
    if cnds:
        cnd = cnds[-1].strip().lower()
        result["cnd_status"] = "negativa" if "negativa" in cnd else "positiva" if "positiva" in cnd else "nao_emitida"

    # --- Restrições ---
    restricoes = re.findall(r"RESTRI[CÇ][OÕ]ES:\s*(.*?)(?:\n|•|$)", free_text, re.IGNORECASE)
    if restricoes:
        last_rest = restricoes[-1].strip()
        result["tem_restricoes"] = "sem" not in last_rest.lower()
        # Protestos
        protestos = re.findall(r"(\d+)\s+PROTESTO", last_rest, re.IGNORECASE)
        if protestos:
            result["qtd_protestos"] = int(protestos[0])
        valor_prot = re.findall(r"R\$\s*([\d.,]+)", last_rest)
        if valor_prot:
            result["valor_protestos"] = parse_money(valor_prot[0])

    # --- QSA (quantidade de sócios) ---
    socios = re.findall(r"(\d+)\s+S[oó]cios?", free_text, re.IGNORECASE)
    if socios:
        result["qtd_socios"] = int(socios[-1])

    # --- Decisão final (última menção) ---
    # Padrões: "LIMITE FINAL R$ 20 MIL", "À VISTA", "LIMITE ZERADO"
    decisoes_vista = re.findall(r"(?:LIMITE\s+FINAL\s+)?(?:SOMENTE\s+)?[AÀ]\s+VISTA", free_text, re.IGNORECASE)
    decisoes_zerado = re.findall(r"LIMITE\s+ZERADO", free_text, re.IGNORECASE)
    decisoes_valor = re.findall(r"LIMITE\s+FINAL\s+(?:DE\s+)?R?\$?\s*([\d.,]+\s*(?:MIL)?)", free_text, re.IGNORECASE)
    limites_aprovados = re.findall(r"[Ll]imite.*?(?:aprovad|alter|mantid|aument).*?R\$\s*([\d.,]+)", free_text)

    if decisoes_zerado:
        result["decisao_final"] = "limite_zerado"
    elif decisoes_vista:
        result["decisao_final"] = "apenas_vista"
    elif decisoes_valor or limites_aprovados:
        result["decisao_final"] = "aprovado_prazo"

    # --- Último analista e data ---
    analistas = re.findall(
        r"(Amanda|Fernanda\s+Kosteke?|Vit[oó]ria|Bruna|Fabiano)\s*[-–]?\s*(\d{2}/\d{2}/\d{4})",
        free_text, re.IGNORECASE
    )
    if analistas:
        result["ultimo_analista"] = analistas[-1][0].strip()
        result["ultima_analise_data"] = analistas[-1][1]

    # --- Recebimentos SAP ---
    recebimentos = re.findall(r"RECEBIMENTOS SAP:\s*R\$\s*([\d.,]+)", free_text, re.IGNORECASE)
    if recebimentos:
        result["recebimentos_sap"] = parse_money(recebimentos[-1])

    # --- Títulos ---
    titulos = re.findall(r"QUANTIDADE DE T[IÍ]TULOS:\s*(\d+)", free_text, re.IGNORECASE)
    if titulos:
        result["qtd_titulos"] = int(titulos[-1])

    titulos_atrasados = re.findall(r"T[IÍ]TULOS\s+N?O?\s*ATRASADOS?:\s*(\d+)", free_text, re.IGNORECASE)
    if titulos_atrasados:
        result["qtd_titulos_atrasados"] = int(titulos_atrasados[-1])

    return result


def empty_result() -> dict:
    return {
        "score_credito": None,
        "score_is_default": None,
        "prob_inadimplencia_pct": None,
        "risco_credito": None,
        "pratica_mercado": None,
        "apenas_vista": None,
        "limite_sugerido_pj": None,
        "limite_aprovado": None,
        "valor_em_aberto": None,
        "tempo_cnpj_anos": None,
        "data_fundacao": None,
        "capital_social": None,
        "faturamento_presumido": None,
        "cnd_status": None,
        "tem_restricoes": None,
        "qtd_protestos": None,
        "valor_protestos": None,
        "qtd_socios": None,
        "decisao_final": None,
        "ultimo_analista": None,
        "ultima_analise_data": None,
        "recebimentos_sap": None,
        "qtd_titulos": None,
        "qtd_titulos_atrasados": None,
    }


# Schema de saída
CREDIT_SCHEMA = StructType([
    StructField("score_credito", IntegerType()),
    StructField("score_is_default", BooleanType()),
    StructField("prob_inadimplencia_pct", DoubleType()),
    StructField("risco_credito", StringType()),
    StructField("pratica_mercado", StringType()),
    StructField("apenas_vista", BooleanType()),
    StructField("limite_sugerido_pj", DoubleType()),
    StructField("limite_aprovado", DoubleType()),
    StructField("valor_em_aberto", DoubleType()),
    StructField("tempo_cnpj_anos", IntegerType()),
    StructField("data_fundacao", StringType()),
    StructField("capital_social", DoubleType()),
    StructField("faturamento_presumido", DoubleType()),
    StructField("cnd_status", StringType()),
    StructField("tem_restricoes", BooleanType()),
    StructField("qtd_protestos", IntegerType()),
    StructField("valor_protestos", DoubleType()),
    StructField("qtd_socios", IntegerType()),
    StructField("decisao_final", StringType()),
    StructField("ultimo_analista", StringType()),
    StructField("ultima_analise_data", StringType()),
    StructField("recebimentos_sap", DoubleType()),
    StructField("qtd_titulos", IntegerType()),
    StructField("qtd_titulos_atrasados", IntegerType()),
])


def build_credit_features(spark):
    """
    Extrai features de crédito do FreeText de BusinessPartners.
    Lê da Silver, parseia, gera tabela Gold estruturada.
    """
    silver_bp = f"{ICEBERG_CATALOG}.{CATALOG_DATABASE_SILVER}.business_partners"
    bp_df = spark.table(silver_bp)

    # Somente versão corrente (SCD2)
    if "_is_current" in bp_df.columns:
        bp_df = bp_df.filter(col("_is_current") == True)

    # Verifica se FreeText existe
    if "freetext" not in [c.lower() for c in bp_df.columns]:
        print("[WARN] Coluna 'FreeText' não encontrada na Silver. Abortando.")
        return

    # UDF para extração
    @udf(returnType=CREDIT_SCHEMA)
    def parse_freetext_udf(text):
        from pyspark.sql import Row
        result = extract_last_analysis(text)
        return Row(**result)

    # Extrai features
    df = (
        bp_df
        .select("cardcode", "cardname", "freetext")
        .withColumn("_features", parse_freetext_udf(col("freetext")))
        .select(
            col("cardcode").alias("cod_cliente"),
            col("cardname").alias("nome_cliente"),
            col("_features.score_credito"),
            col("_features.score_is_default"),
            col("_features.prob_inadimplencia_pct"),
            col("_features.risco_credito"),
            col("_features.pratica_mercado"),
            col("_features.apenas_vista"),
            col("_features.limite_sugerido_pj"),
            col("_features.limite_aprovado"),
            col("_features.valor_em_aberto"),
            col("_features.tempo_cnpj_anos"),
            col("_features.data_fundacao"),
            col("_features.capital_social"),
            col("_features.faturamento_presumido"),
            col("_features.cnd_status"),
            col("_features.tem_restricoes"),
            col("_features.qtd_protestos"),
            col("_features.valor_protestos"),
            col("_features.qtd_socios"),
            col("_features.decisao_final"),
            col("_features.ultimo_analista"),
            col("_features.ultima_analise_data"),
            col("_features.recebimentos_sap"),
            col("_features.qtd_titulos"),
            col("_features.qtd_titulos_atrasados"),
        )
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    # Escreve na Gold (domínio: cadastros)
    full_table = get_gold_table_path("analise_credito")
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    df.writeTo(full_table).using("iceberg").createOrReplace()
    print(f"[GOLD] analise_credito: {df.count()} registros.")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    spark = get_spark_session(args["JOB_NAME"])

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{GOLD_DATABASES['cadastros']['database']}"
    )

    print("\n" + "=" * 60)
    print("  GOLD — Análise de Crédito (NLP Extraction)")
    print("=" * 60)

    build_credit_features(spark)

    spark.stop()
    print("\n[COMPLETE] Job Gold Análise de Crédito finalizado.")


if __name__ == "__main__":
    main()
