"""
Utilitários compartilhados para transformações Bronze → Silver.
Lida com a estrutura real da Bronze:
- JSON arrays na raiz [...]
- Particionamento: {entity}/{full|incremental}/year=YYYY/month=MM/day=DD/
- Múltiplos arquivos full + incrementais
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, sha2, concat_ws,
    when, to_date, to_timestamp, trim, year, month,
    input_file_name, coalesce
)
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc


def get_spark_session(app_name: str) -> SparkSession:
    """Cria SparkSession com Iceberg + Glue Catalog."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://gpcorp-datalake/Silver/warehouse")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )


def read_bronze_json(spark: SparkSession, base_path: str, load_type: str = "all") -> DataFrame:
    """
    Lê dados JSON da camada Bronze.

    Estratégia de leitura:
    - load_type="full": lê apenas full load (carga inicial)
    - load_type="incremental": lê apenas incrementais
    - load_type="all": lê full + incremental (usado para consolidação completa)

    Os JSONs são arrays na raiz, particionados por year/month/day no path.
    Spark infere schema automaticamente; adicionamos _source_file para rastreabilidade.
    """
    if load_type == "full":
        path = f"{base_path}full/"
    elif load_type == "incremental":
        path = f"{base_path}incremental/"
    else:
        # Lê tudo (full + incremental)
        path = base_path

    df = (
        spark.read
        .option("multiline", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(path)
    )

    # Adiciona metadado de rastreabilidade
    df = df.withColumn("_source_file", input_file_name())

    return df


def deduplicate(df: DataFrame, primary_key: list, order_col: str = "_source_file") -> DataFrame:
    """
    Remove duplicatas mantendo o registro mais recente.
    Ordena por _source_file desc (incrementais vêm depois de full no path).
    Garante idempotência no reprocessamento.
    """
    window = Window.partitionBy(*primary_key).orderBy(desc(order_col))
    return (
        df.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


def add_silver_metadata(df: DataFrame) -> DataFrame:
    """Adiciona colunas de controle da camada Silver."""
    return (
        df
        .withColumn("_silver_loaded_at", current_timestamp())
        .withColumn("_silver_load_date", to_date(current_timestamp()))
    )


def generate_surrogate_key(df: DataFrame, key_columns: list, key_name: str = "_sk") -> DataFrame:
    """Gera surrogate key baseada em hash SHA-256 das colunas de negócio."""
    return df.withColumn(
        key_name,
        sha2(concat_ws("||", *[col(c).cast("string") for c in key_columns]), 256)
    )


def add_date_partitions(df: DataFrame, date_col: str) -> DataFrame:
    """Adiciona colunas year/month para particionamento Iceberg."""
    return (
        df
        .withColumn("year", year(to_date(col(date_col))))
        .withColumn("month", month(to_date(col(date_col))))
    )


def mask_pii(df: DataFrame, pii_columns: list) -> DataFrame:
    """
    Mascara colunas PII com hash SHA-256 (LGPD).
    NULL permanece NULL.
    """
    for column in pii_columns:
        if column in df.columns:
            df = df.withColumn(
                column,
                when(col(column).isNotNull(), sha2(col(column).cast("string"), 256))
                .otherwise(lit(None))
            )
    return df


def clean_strings(df: DataFrame) -> DataFrame:
    """Trim em todas as colunas string do DataFrame."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df


def validate_not_null_keys(df: DataFrame, primary_key: list) -> DataFrame:
    """
    Remove registros com chave primária nula.
    Loga quantidade removida para auditoria (CloudWatch).
    """
    condition = None
    for key in primary_key:
        key_check = col(key).isNotNull()
        condition = key_check if condition is None else (condition & key_check)

    valid_df = df.filter(condition)
    total = df.count()
    valid_count = valid_df.count()
    invalid_count = total - valid_count

    if invalid_count > 0:
        pct = (valid_count / total) * 100 if total > 0 else 0
        print(f"[QUALITY] {invalid_count} registros removidos (PK nula: {primary_key}). "
              f"Taxa válida: {pct:.2f}%")

    return valid_df
