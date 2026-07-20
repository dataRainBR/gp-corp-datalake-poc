"""
SageMaker - Dataset Builder para Modelo de Série Temporal (Previsão de Vendas)

Consome dados da camada Silver via Amazon Athena e constrói o dataset
desnormalizado para o modelo de predição de vendas GP Corp.

Otimizações de performance:
- Usa PyAthena com cursor descompactado (PandasCursor) para leitura direta em DataFrame
- Aproveita partições (year, month) para evitar full scan
- Executa query com CTAS ou Unload para grandes volumes
- Cache de resultados via workgroup configurado no Athena
"""

import os
import time
from datetime import datetime
from typing import Optional

import boto3
import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

from sagemaker_logs import show_log

# Constantes - ajustar conforme ambiente
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "gpcorp_silver")
ATHENA_S3_OUTPUT = os.environ.get("ATHENA_S3_OUTPUT", "s3://gpcorp-athena-results/sagemaker/")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Caminho local para salvar o dataset (SageMaker processing job ou notebook)
OUTPUT_PATH= os.environ.get("OUTPUT_DIR", "/mnt/custom-file-systems/s3/shared/processing/dataset")
# ---------------------------------------------------------------------------
# Query SQL para construção do dataset base
# ---------------------------------------------------------------------------

DATASET_QUERY = """
SELECT
    inv.docdate              AS data_venda,
    inv.itemcode             AS item_code,
    it.itemname              AS item_name,
    ig.groupname             AS item_group,
    inv.quantity             AS quantidade,
    inv.linetotal            AS valor_total,
    inv.unitprice            AS preco_unitario,
    inv.discountpercent      AS desconto_pct,
    inv.salespersoncode      AS vendedor_code,
    sp.salesemployeename     AS vendedor_nome,
    inv.branchid             AS filial_id,
    inv.cardcode             AS cliente_code,
    inv.cardname             AS cliente_nome,
    bp.city                  AS cliente_cidade,
    bp.billtostate           AS cliente_uf,
    inv.warehousecode        AS armazem,
    inv.grossprofit          AS lucro_bruto,
    inv.year                 AS ano,
    inv.month                AS mes,
    inv.day                  AS dia
FROM {database}.invoices inv
LEFT JOIN {database}.items it
    ON inv.itemcode = it.itemcode
LEFT JOIN {database}.item_groups ig
    ON it.itemsgroupcode = ig.number
LEFT JOIN {database}.sales_persons sp
    ON inv.salespersoncode = sp.salesemployeecode
LEFT JOIN {database}.business_partners bp
    ON inv.cardcode = bp.cardcode
WHERE inv.documentstatus = 'bost_Close'
  AND inv.cancelled = 'tNO'
  AND inv.doctotal > 0
  AND inv.quantity > 0
  AND inv.freeofchargebp = 'tNO'
  AND it.salesitem = 'tYES'
"""

# Filtro de partição para reduzir escaneamento
PARTITION_FILTER = """
  AND inv.year >= {year_start}
"""


# ---------------------------------------------------------------------------
# Funções
# ---------------------------------------------------------------------------


def get_athena_connection(region: str = AWS_REGION):
    """
    Cria conexão com Athena usando PandasCursor para performance otimizada.

    O PandasCursor faz a desserialização diretamente em DataFrame,
    evitando overhead de conversão intermediária.
    """
    return connect(
        s3_staging_dir=ATHENA_S3_OUTPUT,
        region_name=region,
        work_group=ATHENA_WORKGROUP,
        cursor_class=PandasCursor,
    )


def build_query(
    year_start: Optional[int] = None,
    month_start: Optional[int] = None,
    year_end: Optional[int] = None,
    month_end: Optional[int] = None,
    limit: Optional[int] = None,
) -> str:
    """
    Monta a query SQL com filtros de partição opcionais para performance.

    Parameters
    ----------
    year_start : int, optional
        Ano mínimo para filtro de partição (ex: 2022).
    month_start : int, optional
        Mês mínimo (usado junto com year_start).
    year_end : int, optional
        Ano máximo para filtro de partição.
    month_end : int, optional
        Mês máximo (usado junto com year_end).
    limit : int, optional
        Limitar número de registros retornados (para testes).

    Returns
    -------
    str
        Query SQL formatada com filtros de partição.
    """
    query = DATASET_QUERY.format(database=ATHENA_DATABASE)

    # Filtros de partição para reduzir dados escaneados (custo e tempo)
    if year_start is not None:
        query += f"\n  AND inv.year >= {year_start}"
    if year_end is not None:
        query += f"\n  AND inv.year <= {year_end}"
    if year_start is not None and month_start is not None:
        query += f"\n  AND (inv.year > {year_start} OR (inv.year = {year_start} AND inv.month >= {month_start}))"
    if year_end is not None and month_end is not None:
        query += f"\n  AND (inv.year < {year_end} OR (inv.year = {year_end} AND inv.month <= {month_end}))"

    query += "\nORDER BY inv.docdate, inv.itemcode"

    if limit is not None:
        query += f"\nLIMIT {limit}"

    return query


def fetch_dataset(
    year_start: Optional[int] = None,
    month_start: Optional[int] = None,
    year_end: Optional[int] = None,
    month_end: Optional[int] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Executa a query no Athena e retorna o dataset como DataFrame.

    Usa PandasCursor para conversão direta — sem etapas intermediárias.
    O Athena utiliza partições year/month para minimizar o scan de dados.

    Parameters
    ----------
    year_start, month_start, year_end, month_end : int, optional
        Filtros de partição temporal.
    limit : int, optional
        Limitar registros (para testes).

    Returns
    -------
    pd.DataFrame
        Dataset desnormalizado pronto para engenharia de features.
    """
    query = build_query(year_start, month_start, year_end, month_end, limit=limit)

    show_log("Executando query no Athena...")
    show_log(f"Database: {ATHENA_DATABASE}")
    show_log(f"Workgroup: {ATHENA_WORKGROUP}")
    show_log(f"Output S3: {ATHENA_S3_OUTPUT}")
    show_log(f"Query gerada:\n{query}")

    start_time = time.time()

    conn = get_athena_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    elapsed = time.time() - start_time
    show_log(f"Query executada em {elapsed:.2f}s — {len(df):,} registros retornados")

    return df


def fetch_dataset_chunked(
    year_start: int,
    year_end: int,
    chunk_by: str = "year",
) -> pd.DataFrame:
    """
    Busca o dataset em chunks (por ano ou por mês) para grandes volumes.

    Útil quando o volume total excede a memória ou timeout do Athena.
    Cada chunk usa filtro de partição nativo, mantendo performance.

    Parameters
    ----------
    year_start : int
        Ano inicial.
    year_end : int
        Ano final (inclusive).
    chunk_by : str
        'year' para chunks anuais, 'month' para mensais.

    Returns
    -------
    pd.DataFrame
        Dataset completo concatenado.
    """
    frames = []

    if chunk_by == "year":
        for year in range(year_start, year_end + 1):
            show_log(f"Buscando dados do ano {year}...")
            df_chunk = fetch_dataset(year_start=year, year_end=year)
            frames.append(df_chunk)
            show_log(f"  Ano {year}: {len(df_chunk):,} registros")
    elif chunk_by == "month":
        for year in range(year_start, year_end + 1):
            for month in range(1, 13):
                show_log(f"Buscando {year}-{month:02d}...")
                df_chunk = fetch_dataset(
                    year_start=year,
                    month_start=month,
                    year_end=year,
                    month_end=month,
                )
                if not df_chunk.empty:
                    frames.append(df_chunk)
                    show_log(f"  {year}-{month:02d}: {len(df_chunk):,} registros")

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    show_log(f"Total consolidado: {len(df):,} registros")
    return df


def apply_quality_filters(df: pd.DataFrame, min_sales: int = 10) -> pd.DataFrame:
    """
    Aplica filtros de qualidade no dataset para remover ruído.

    Parameters
    ----------
    df : pd.DataFrame
        Dataset bruto do Athena.
    min_sales : int
        Número mínimo de vendas por item para inclusão no modelo.

    Returns
    -------
    pd.DataFrame
        Dataset filtrado.
    """
    initial_count = len(df)

    # Remover itens com poucas vendas (dados insuficientes para série temporal)
    item_counts = df["item_code"].value_counts()
    valid_items = item_counts[item_counts >= min_sales].index
    df = df[df["item_code"].isin(valid_items)].copy()
    show_log(
        f"Filtro min_sales={min_sales}: {initial_count:,} → {len(df):,} registros "
        f"({len(valid_items):,} itens mantidos)"
    )

    # Remover outliers por produto (quantity > 3 desvios padrão)
    before_outlier = len(df)
    stats = df.groupby("item_code")["quantidade"].agg(["mean", "std"]).reset_index()
    stats.columns = ["item_code", "qty_mean", "qty_std"]
    df = df.merge(stats, on="item_code", how="left")
    df = df[df["quantidade"] <= (df["qty_mean"] + 3 * df["qty_std"])].copy()
    df.drop(columns=["qty_mean", "qty_std"], inplace=True)
    show_log(
        f"Filtro outliers (3σ): {before_outlier:,} → {len(df):,} registros "
        f"(removidos {before_outlier - len(df):,})"
    )

    return df.reset_index(drop=True)


def save_dataset(df: pd.DataFrame, output_path: str = OUTPUT_PATH, filename: str = "dataset_serie_temporal.parquet"):
    """
    Salva o dataset em formato Parquet (otimizado para leitura no treino).

    Parameters
    ----------
    df : pd.DataFrame
        Dataset a ser salvo.
    output_path : str
        Diretório de saída.
    filename : str
        Nome do arquivo.
    """
    os.makedirs(output_path, exist_ok=True)
    filepath = os.path.join(output_path, filename)
    df.to_parquet(filepath, index=False, engine="pyarrow")
    show_log(f"Dataset salvo em: {filepath} ({len(df):,} registros)")
    return filepath


# ---------------------------------------------------------------------------
# Entrypoint principal (para SageMaker Processing Job ou execução direta)
# ---------------------------------------------------------------------------


def dataset_builder(year_start, year_end, month_start, month_end, output_path, min_sales=0, chunked=False, output_format="parquet"):
    """
    Pipeline principal de construção do dataset para o modelo de série temporal.

    Etapas:
    1. Consulta Athena (camada Silver) com filtros de partição
    2. Aplica filtros de qualidade (min vendas, outliers)
    3. Salva em Parquet para consumo no treino
    """

    show_log("=" * 60)
    show_log("GP Corp - Dataset Builder para Série Temporal")
    show_log("=" * 60)

    # 1. Buscar dados do Athena
    if chunked and year_start and year_end:
        df = fetch_dataset_chunked(year_start=year_start, year_end=year_end)
    else:
        df = fetch_dataset(
            year_start=year_start,
            month_start=month_start,
            year_end=year_end,
            month_end=month_end,
        )

    if df.empty:
        show_log("Nenhum dado retornado. Verifique os filtros e a disponibilidade dos dados.")
        return

    # 2. Info do dataset
    show_log(f"\nShape do dataset: {df.shape}")
    show_log(f"Período: {df['data_venda'].min()} a {df['data_venda'].max()}")
    show_log(f"Itens únicos: {df['item_code'].nunique():,}")
    show_log(f"Vendedores únicos: {df['vendedor_code'].nunique():,}")

    # 3. Filtros de qualidade
    #df = apply_quality_filters(df, min_sales=args.min_sales)

    # 4. Salvar
    if output_format == "parquet":
        filepath = os.path.join(output_path, "dataset_serie_temporal.parquet")
        save_dataset(df, output_path=output_path)
    elif output_format == "csv":
        filepath = os.path.join(output_path, "dataset_serie_temporal.csv")
        df.to_csv(filepath, index=False)
        show_log(f"Dataset salvo em: {filepath}")

    else:
        show_log(f"Formato não suportado! Arquivo não salvo")
        raise
        
    show_log("\nDataset pronto para engenharia de features temporais.")
    show_log("=" * 60)



def main():
    """
    Pipeline principal de construção do dataset para o modelo de série temporal.

    Etapas:
    1. Consulta Athena (camada Silver) com filtros de partição
    2. Aplica filtros de qualidade (min vendas, outliers)
    3. Salva em Parquet para consumo no treino
    """
    import argparse

    parser = argparse.ArgumentParser(description="Build time series dataset from Athena Silver layer")
    parser.add_argument("--year-start", type=int, default=None, help="Ano inicial (filtro de partição)")
    parser.add_argument("--year-end", type=int, default=None, help="Ano final (filtro de partição)")
    parser.add_argument("--month-start", type=int, default=None, help="Mês inicial")
    parser.add_argument("--month-end", type=int, default=None, help="Mês final")
    parser.add_argument("--min-sales", type=int, default=10, help="Mínimo de vendas por item")
    parser.add_argument("--chunked", action="store_true", help="Buscar em chunks por ano")
    parser.add_argument("--output-path", type=str, default=OUTPUT_PATH, help="Diretório de saída")
    parser.add_argument("--output-format", type=str, default="parquet", choices=["parquet", "csv"], help="Formato de saída")
    args = parser.parse_args()

    show_log("=" * 60)
    show_log("GP Corp - Dataset Builder para Série Temporal")
    show_log("=" * 60)

    # 1. Buscar dados do Athena
    if args.chunked and args.year_start and args.year_end:
        df = fetch_dataset_chunked(year_start=args.year_start, year_end=args.year_end)
    else:
        df = fetch_dataset(
            year_start=args.year_start,
            month_start=args.month_start,
            year_end=args.year_end,
            month_end=args.month_end,
        )

    if df.empty:
        show_log("Nenhum dado retornado. Verifique os filtros e a disponibilidade dos dados.")
        return

    # 2. Info do dataset
    show_log(f"\nShape do dataset: {df.shape}")
    show_log(f"Período: {df['data_venda'].min()} a {df['data_venda'].max()}")
    show_log(f"Itens únicos: {df['item_code'].nunique():,}")
    show_log(f"Vendedores únicos: {df['vendedor_code'].nunique():,}")

    # 3. Filtros de qualidade
    df = apply_quality_filters(df, min_sales=args.min_sales)

    # 4. Salvar
    if args.output_format == "parquet":
        save_dataset(df, output_path=args.output_path)
    else:
        os.makedirs(args.output_path, exist_ok=True)
        filepath = os.path.join(args.output_path, "dataset_serie_temporal.csv")
        df.to_csv(filepath, index=False)
        show_log(f"Dataset salvo em: {filepath}")

    show_log("\nDataset pronto para engenharia de features temporais.")
    show_log("=" * 60)





if __name__ == "__main__":
    main()
