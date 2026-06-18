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
    input_file_name, coalesce, regexp_replace
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


def read_bronze_json(spark: SparkSession, base_path: str, load_type: str = "all",
                     load_date: str = None) -> DataFrame:
    """
    Lê dados JSON da camada Bronze.

    Estratégia de leitura:
    - load_type="full": lê apenas full load (carga inicial)
    - load_type="incremental": lê incrementais
        - se load_date="YYYY-MM-DD": lê apenas a partição daquele dia
        - senão: lê todos os incrementais
    - load_type="all": lê full + incremental (consolidação completa)

    Os JSONs são arrays na raiz, particionados por year/month/day no path.
    """
    if load_type == "full":
        path = f"{base_path}full/"
    elif load_type == "incremental":
        if load_date:
            y, m, d = load_date.split("-")
            path = f"{base_path}incremental/year={y}/month={m}/day={d}/"
            print(f"[READ] Carga incremental do dia {load_date}: {path}")
        else:
            path = f"{base_path}incremental/"

        # Tenta ler incremental; se não existir, retorna DataFrame vazio ou lê full
        try:
            df = (
                spark.read
                .option("multiline", "true")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(path)
            )
            df = df.withColumn("_source_file", input_file_name())
            return df
        except Exception as e:
            print(f"[WARN] Path incremental não existe: {path}. Pulando entidade.")
            # Retorna None — o job deve tratar como "sem dados novos"
            return None
    else:
        # Lê full como base principal, depois incremental para overlay via dedup
        df_full = None
        df_inc = None

        try:
            df_full = (
                spark.read
                .option("multiline", "true")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(f"{base_path}full/")
            )
            df_full = df_full.withColumn("_source_file", input_file_name())
        except Exception as e:
            print(f"[WARN] Falha ao ler full/: {e}")

        try:
            df_inc = (
                spark.read
                .option("multiline", "true")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(f"{base_path}incremental/")
            )
            df_inc = df_inc.withColumn("_source_file", input_file_name())
        except Exception as e:
            print(f"[WARN] Falha ao ler incremental/: {e}")

        if df_full is not None and df_inc is not None:
            # Union seguro: projeta colunas do full no incremental (full define o schema)
            full_cols = df_full.columns
            # Seleciona apenas colunas que existem no inc, alinha pelo schema do full
            try:
                df_inc_aligned = df_inc.select(*[col(f"`{c}`") for c in full_cols if c in df_inc.columns])
                df = df_full.unionByName(df_inc_aligned, allowMissingColumns=True)
            except Exception as e:
                # Se union falhar (tipos incompatíveis), usa apenas full
                print(f"[WARN] Union falhou, usando apenas full: {e}")
                df = df_full
        elif df_full is not None:
            df = df_full
        elif df_inc is not None:
            df = df_inc
        else:
            raise Exception(f"Nenhum dado encontrado em {base_path}")

        return df

    df = (
        spark.read
        .option("multiline", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(path)
    )

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
    """Adiciona colunas year/month/day para particionamento Iceberg."""
    from pyspark.sql.functions import dayofmonth
    return (
        df
        .withColumn("year", year(to_date(col(date_col))))
        .withColumn("month", month(to_date(col(date_col))))
        .withColumn("day", dayofmonth(to_date(col(date_col))))
    )


def mask_pii(df: DataFrame, pii_columns: list, condition_col: str = None, condition_val: str = None) -> DataFrame:
    """
    Mascara colunas PII com hash SHA-256 (LGPD).
    NULL permanece NULL.

    Se condition_col e condition_val forem informados, mascara apenas
    registros onde condition_col == condition_val (ex: CardType com CPF).
    """
    for column in pii_columns:
        if column in df.columns:
            if condition_col and condition_val:
                # Mascara apenas quando condição é verdadeira
                df = df.withColumn(
                    column,
                    when(
                        (col(condition_col) == condition_val) & col(column).isNotNull(),
                        sha2(col(column).cast("string"), 256)
                    ).otherwise(col(column))
                )
            else:
                df = df.withColumn(
                    column,
                    when(col(column).isNotNull(), sha2(col(column).cast("string"), 256))
                    .otherwise(lit(None))
                )
    return df


def clean_strings(df: DataFrame) -> DataFrame:
    """Trim e remove quebras de linha em todas as colunas string do DataFrame."""
    from pyspark.sql.functions import regexp_replace
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(regexp_replace(col(field.name), r"[\r\n]+", " ")))
    return df


def validate_not_null_keys(df: DataFrame, primary_key: list) -> DataFrame:
    """
    Remove registros com chave primária nula.
    Para PKs numéricas, também remove negativos (SAP usa -1 como sentinela).
    """
    from pyspark.sql.types import IntegerType, LongType, ShortType, DoubleType, FloatType

    condition = None
    for key in primary_key:
        key_check = col(key).isNotNull()
        # Remove negativos apenas para colunas numéricas
        try:
            field_type = df.schema[key].dataType
            if isinstance(field_type, (IntegerType, LongType, ShortType, DoubleType, FloatType)):
                key_check = key_check & (col(key) >= 0)
        except Exception:
            pass
        condition = key_check if condition is None else (condition & key_check)

    valid_df = df.filter(condition)
    total = df.count()
    valid_count = valid_df.count()
    invalid_count = total - valid_count

    if invalid_count > 0:
        pct = (valid_count / total) * 100 if total > 0 else 0
        print(f"[QUALITY] {invalid_count} registros removidos (PK nula ou negativa: {primary_key}). "
              f"Taxa válida: {pct:.2f}%")

    return valid_df
