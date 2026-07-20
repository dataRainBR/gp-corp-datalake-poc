"""
SageMaker - Inference Pipeline para Série Temporal (Previsão de Vendas)

Etapas:
1. Carrega modelo mais recente do S3
2. Busca últimos 30 dias do Athena
3. Aplica preprocessamento
4. Faz predições para os próximos 30 dias
5. Monta dataset consolidado (histórico + predições)
6. Salva no S3 Gold particionado
7. Avalia predições e gera gráficos

Uso no notebook:
    from sagemaker_inference import run_inference_pipeline
    results = run_inference_pipeline()
"""

import os
import io
import joblib
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

from sagemaker_dataset_builder import (
    show_log, get_athena_connection,
    ATHENA_DATABASE, ATHENA_S3_OUTPUT, ATHENA_WORKGROUP,
)

from sagemaker_preprocessing import (
    SalesAggregator,
    CalendarCompleter,
    NullHandler,
    TemporalFeatureCreator,
    LagFeatureCreator,
    RollingFeatureCreator,
    DemandFeatureCreator,
    PriceFeatureCreator,
    BusinessFeatureCreator,
    TrendFeatureCreator,
    CategoryEncoder
)

from sagemaker_preprocessing import REMOVE_COLUMNS

from sagemaker_logs import show_log

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

S3_GOLD_PATH = "s3://gpcorp-datalake/Gold/"
MODEL_S3_PREFIX = "models/time-series"
Bucket=None 

GPCORP_COLORS = {
    "azul_escuro": "#1B3A5C",
    "azul_medio": "#2E6B9E",
    "azul_claro": "#5BA4D9",
    "laranja": "#E8772E",
    "laranja_claro": "#F5A623",
    "dourado": "#D4A843",
    "cinza_escuro": "#3D3D3D",
    "cinza_claro": "#A8A8A8",
    "branco": "#FFFFFF",
}

GPCORP_SEQUENCE = [
    GPCORP_COLORS["azul_escuro"],
    GPCORP_COLORS["laranja"],
    GPCORP_COLORS["azul_medio"],
    GPCORP_COLORS["dourado"],
    GPCORP_COLORS["azul_claro"],
    GPCORP_COLORS["laranja_claro"],
]


# ---------------------------------------------------------------------------
# Estilo GP Corp
# ---------------------------------------------------------------------------


def _apply_gpcorp_style(fig, title: str = "", height: int = 500):
    fig.update_layout(
        title=title,
        title_font=dict(size=16, color=GPCORP_COLORS["azul_escuro"]),
        font=dict(family="Segoe UI, Arial, sans-serif", color=GPCORP_COLORS["cinza_escuro"]),
        paper_bgcolor=GPCORP_COLORS["branco"],
        plot_bgcolor="#F8F9FA",
        height=height,
        margin=dict(t=60, b=40, l=60, r=30),
    )
    fig.update_xaxes(gridcolor="#E0E0E0", gridwidth=0.5)
    fig.update_yaxes(gridcolor="#E0E0E0", gridwidth=0.5)
    return fig


# ---------------------------------------------------------------------------
# 1. Helpers S3
# ---------------------------------------------------------------------------


def get_default_bucket() -> str:
    """Retorna o bucket padrão do SageMaker."""
    sts = boto3.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    region = boto3.session.Session().region_name or "us-east-1"
    return f"sagemaker-{region}-{account_id}"


def _ensure_bucket(s3_client, bucket: str):
    """Cria bucket se não existir."""
    try:
        s3_client.head_bucket(Bucket=bucket)
    except Exception:
        show_log(f"Bucket '{bucket}' não existe. Criando...")
        region = boto3.session.Session().region_name or "us-east-1"
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket)
        else:
            s3_client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        show_log(f"Bucket '{bucket}' criado.")


# ---------------------------------------------------------------------------
# 2. Carregar modelo e pipeline do S3
# ---------------------------------------------------------------------------


def load_latest_model(
    model_name: str = "xgboost_quantidade",
    bucket: str = None,
    base_prefix: str = MODEL_S3_PREFIX,
):
    """
    Carrega o modelo XGBoost .pkl mais recente do S3.
    Ignora arquivos preprocessing_pipeline.pkl.
    """
    if bucket is None:
        bucket = get_default_bucket()

    s3_client = boto3.client("s3")
    prefix = f"{base_prefix}/{model_name}/"
    show_log(f"Buscando modelo em s3://{bucket}/{prefix}")

    paginator = s3_client.get_paginator("list_objects_v2")
    pkl_files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.split("/")[-1]
            # Aceita apenas arquivos cujo nome começa com model_name e termina em .pkl
            if filename.startswith(model_name) and filename.endswith(".pkl"):
                pkl_files.append(obj)

    if not pkl_files:
        raise FileNotFoundError(f"Nenhum modelo encontrado em s3://{bucket}/{prefix}")

    pkl_files.sort(key=lambda x: x["LastModified"], reverse=True)
    latest = pkl_files[0]
    show_log(f"Modelo mais recente: {latest['Key']} ({latest['LastModified']})")

    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
        tmp_path = tmp.name
        s3_client.download_file(bucket, latest["Key"], tmp_path)

    model = joblib.load(tmp_path)
    os.remove(tmp_path)
    show_log("Modelo carregado com sucesso.")
    return model


def load_preprocessing_pipeline(
    model_name: str = "xgboost_quantidade",
    bucket: str = None,
    base_prefix: str = MODEL_S3_PREFIX,
):
    """
    Carrega o pipeline de preprocessamento mais recente do S3.
    Retorna None se não encontrado.
    """
    if bucket is None:
        bucket = get_default_bucket()

    s3_client = boto3.client("s3")
    prefix = f"{base_prefix}/{model_name}/"
    show_log(f"Buscando pipeline em s3://{bucket}/{prefix}")

    paginator = s3_client.get_paginator("list_objects_v2")
    pipeline_files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("preprocessing_pipeline.pkl"):
                pipeline_files.append(obj)

    if not pipeline_files:
        show_log("Pipeline de preprocessamento não encontrado no S3.")
        return None

    pipeline_files.sort(key=lambda x: x["LastModified"], reverse=True)
    latest = pipeline_files[0]
    show_log(f"Pipeline encontrado: {latest['Key']} ({latest['LastModified']})")

    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tmp:
        tmp_path = tmp.name
        s3_client.download_file(bucket, latest["Key"], tmp_path)

    pipeline = joblib.load(tmp_path)
    os.remove(tmp_path)
    show_log("Pipeline carregado com sucesso.")
    return pipeline


def load_model_and_pipeline(
    model_name: str = "xgboost_quantidade",
    bucket: str = None,
    base_prefix: str = MODEL_S3_PREFIX,
) -> Tuple:
    """
    Carrega modelo XGBoost e pipeline de preprocessamento em uma única chamada.

    Returns
    -------
    Tuple[Pipeline, Pipeline]
        (model, preprocessing_pipeline)
    """
    show_log("=" * 60)
    show_log(f"Carregando modelo e pipeline: {model_name}")
    show_log("=" * 60)

    model = load_latest_model(model_name=model_name, bucket=bucket, base_prefix=base_prefix)
    preproc = load_preprocessing_pipeline(model_name=model_name, bucket=bucket, base_prefix=base_prefix)

    if preproc is None:
        show_log("AVISO: pipeline de preprocessamento não encontrado.")

    show_log(f"Modelo steps:   {[s[0] for s in model.steps]}")
    if preproc:
        show_log(f"Pipeline steps: {[s[0] for s in preproc.steps]}")
    show_log("=" * 60)

    return model, preproc


# ---------------------------------------------------------------------------
# 3. Buscar últimos 30 dias do Athena
# ---------------------------------------------------------------------------


def fetch_last_days(database: str = ATHENA_DATABASE, days=90) -> pd.DataFrame:
    """
    Busca os dados dos últimos 90 dias da camada Silver via Athena.

    Returns
    -------
    pd.DataFrame
        Dataset dos últimos 90 dias com todas as colunas do modelo.
    """
    date_ago = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    query = f"""
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
    LEFT JOIN {database}.items it ON inv.itemcode = it.itemcode
    LEFT JOIN {database}.item_groups ig ON it.itemsgroupcode = ig.number
    LEFT JOIN {database}.sales_persons sp ON inv.salespersoncode = sp.salesemployeecode
    LEFT JOIN {database}.business_partners bp ON inv.cardcode = bp.cardcode
    WHERE inv.documentstatus = 'bost_Close'
      AND inv.cancelled = 'tNO'
      AND inv.doctotal > 0
      AND inv.quantity > 0
      AND inv.freeofchargebp = 'tNO'
      AND it.salesitem = 'tYES'
      AND inv.docdate >= DATE('{date_ago}')
    ORDER BY inv.docdate, inv.itemcode
    """

    show_log(f"Buscando dados dos últimos {days} dias (desde {date_ago})...")
    conn = get_athena_connection()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    show_log(f"Dados carregados: {len(df):,} registros")
    return df



# ---------------------------------------------------------------------------
# 5. Gerar intervalos de confiança das predições
# ---------------------------------------------------------------------------
def prediction_interval_bootstrap(forecast, target):
    """
    Calcula estatísticas apenas sobre as previsões
    (linhas com tipo='forecast').
    """

    preds = forecast.loc[
        forecast["tipo"] == "forecast",
        target
    ]

    mean = preds.mean()
    std = preds.std()
    lower = np.percentile(preds, 5)
    upper = np.percentile(preds, 95)

    return mean, lower, upper, std
    
# ---------------------------------------------------------------------------
# 6. Preparar o datset para as predições
# ---------------------------------------------------------------------------
def align_features(
    df: pd.DataFrame,
    feature_cols: List[str],
) -> pd.DataFrame:
    """
    Alinha o DataFrame de inferência com as colunas esperadas pelo modelo.

    - Colunas ausentes (dummies que não apareceram nos dados de inferência)
      são adicionadas com valor 0.
    - Colunas extras (dummies novas que não existiam no treino)
      são removidas.
    - A ordem das colunas é garantida igual ao treino.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame de inferência (já preprocessado/dummizado).
    feature_cols : list
        Lista de colunas que o modelo espera (do treino).

    Returns
    -------
    pd.DataFrame
        DataFrame alinhado com exatamente as colunas do treino.
    """
    df = df.copy()

    missing = [c for c in feature_cols if c not in df.columns]
    extra   = [c for c in df.columns if c not in feature_cols]

    if missing:
        show_log(f"  Colunas ausentes na inferência (adicionadas com 0): {len(missing)}")
        for col in missing:
            df[col] = 0

    if extra:
        show_log(f"  Colunas extras na inferência (removidas): {len(extra)}")
        df = df.drop(columns=extra)

    # Garantir ordem idêntica ao treino
    df = df[feature_cols]

    return df

def prepare_prediction_dataset(
    forecast_dataset: pd.DataFrame,
    inference_pipeline,
    feature_columns: list,
    target: str,
    remove_columns: list,
):
    """
    Gera o dataset de previsão completamente preparado para o modelo.

    Etapas:
        1. Executa todo o pipeline
        2. Alinha as colunas esperadas pelo modelo

    Retorna:
        X_pred
        dataset_processado
    """

    # ----------------------------------------------------------
    # Executa todo o pipeline
    # ----------------------------------------------------------
    #dataset_processed = inference_pipeline.transform(forecast_dataset)
    dataset_processed= forecast_dataset.copy()
    for name, step in inference_pipeline.named_steps.items():

        dataset_processed = step.transform(dataset_processed)
    
        print(
            name,
            dataset_processed.shape,
            dataset_processed["data_venda"].min(),
            dataset_processed["data_venda"].max()
        )

    # ----------------------------------------------------------
    # Seleciona somente o dia a ser previsto
    # ----------------------------------------------------------

    X_pred = dataset_processed.copy()
    target_values= X_pred[['quantidade', 'valor_total']]
    X_pred=remove_columns(X_pred)

    # ----------------------------------------------------------
    # Remove targets
    # ----------------------------------------------------------
    X_pred = X_pred.drop(
        columns=['quantidade', 'valor_total'],
        errors="ignore",
    )

    # ----------------------------------------------------------
    # Garante mesmas colunas do treinamento
    # ----------------------------------------------------------

    X_pred = align_features(
        X_pred,
        feature_columns,
    )

    return X_pred, target_values
    
# ---------------------------------------------------------------------------
# 7. Realizar o Forecast Recursivo
# ---------------------------------------------------------------------------
def recursive_forecast(
    history: pd.DataFrame,
    model,
    inference_pipeline,
    feature_columns,
    remove_columns: list,
    horizon=30,
    target="quantidade",
):

    """
    Forecast recursivo.

    A cada iteração:

        histórico + próximo dia
               ↓
        pipeline completo
               ↓
        previsão
               ↓
        adiciona previsão ao histórico
               ↓
        próximo dia

    """

    history = history.copy()

    history["tipo"] = "historico"

    history["data_venda"] = pd.to_datetime(history["data_venda"])

    history = history.sort_values(
        ["item_code", "filial_id", "data_venda"]
    )

    group_cols = [
        "item_code",
        "filial_id",
        "vendedor_code",
    ]

    show_log("=" * 70)
    show_log("INICIANDO FORECAST RECURSIVO")
    show_log(f"Horizonte: {horizon}")
    show_log(f"Linhas históricas: {len(history):,}")
    show_log("=" * 70)

    predictions = []
    last_day = history["data_venda"].max()

    ############################################################

    for step in range(horizon):

        next_date = history["data_venda"].max() + pd.Timedelta(days=1)

        show_log("")
        show_log("-" * 70)
        show_log(f"Passo {step+1}/{horizon}")
        show_log(f"Data: {next_date.date()}")

        ########################################################
        # Apenas atributos fixos da série
        ########################################################

        future = (

            history

            .sort_values("data_venda")

            .groupby(group_cols)

            .last()

            .reset_index()[
                group_cols
                + [
                    "item_name",
                    "item_group",
                    "vendedor_nome",
                    "armazem",
                    "cliente_uf",
                    "cliente_cidade",
                    "desconto_pct",
                    "preco_unitario",
                ]
            ]

        )

        future["data_venda"] = next_date

        future["tipo"] = "forecast"

        ########################################################
        # Apenas a variável prevista fica vazia
        ########################################################

        future[target] = np.nan

        if target == "quantidade":

            future["valor_total"] = np.nan

        else:

            future["quantidade"] = np.nan

        ########################################################

        dataset = pd.concat(

            [history, future],

            ignore_index=True,

        )

        ########################################################
        # Pipeline completo
        ########################################################

        processed = dataset.copy()

        show_log("Aplicando pipeline...")

        for name, transformer in inference_pipeline.named_steps.items():

            processed = transformer.transform(processed)

            show_log(
                f"{name:<30} {processed.shape}"
            )

        ########################################################

        pred_rows = processed.loc[
            processed["data_venda"] == next_date
        ].copy()

        ########################################################

        X = pred_rows.drop(
            columns=remove_columns,
            errors="ignore",
        )

        X = X.drop(
            columns=[
                "quantidade",
                "valor_total",
            ],
            errors="ignore",
        )

        X = align_features(
            X,
            feature_columns,
        )

        ########################################################

        pred = model.predict(X)

        pred = np.expm1(pred)

        pred = np.clip(pred, 0, None)

        ########################################################

        pred_rows[target] = pred

        show_log(
            f"Min={pred.min():.2f} "
            f"Max={pred.max():.2f} "
            f"Média={pred.mean():.2f}"
        )

        ########################################################

        predictions.append(pred_rows.copy())

        ########################################################
        # Atualiza histórico com as features recalculadas
        ########################################################

        history = pd.concat(

            [

                history,

                pred_rows

            ],

            ignore_index=True,

        )

        show_log(
            f"Histórico atualizado: {len(history):,} linhas"
        )

    history["tipo"] = np.where(
        history["data_venda"] > last_day,
        "forecast",
        "historico"
        )
    forecast = history[history["tipo"] == "forecast"]

    show_log("=" * 70)
    show_log("FORECAST FINALIZADO")
    show_log(f"Linhas previstas: {len(forecast):,}")
    show_log(
        f"Total previsto: {forecast[target].sum():,.2f}"
    )
    show_log("=" * 70)

    return history
    
# ---------------------------------------------------------------------------
# 8. Realizar as predições)
# ---------------------------------------------------------------------------
def make_predictions(
    history,
    model,
    inference_pipeline,
    target,
    days_ahead,
    remove_columns: list,
    feature_cols=None,
):

    show_log("=" * 70)
    show_log("Iniciando Pipeline de Predição")
    show_log("=" * 70)

    history = history.copy()

    history["data_venda"] = pd.to_datetime(history["data_venda"])
    

    ###############################################################
    # Feature names
    ###############################################################

    if feature_cols is None:

        try:

            feature_cols = (
                model.named_steps["xgb"]
                .get_booster()
                .feature_names
            )

        except Exception:

            raise ValueError(
                "feature_cols não encontrado."
            )
            

    ###############################################################
    # Forecast Recursivo
    ###############################################################
    forecast = recursive_forecast(

        history=history,

        model=model,

        inference_pipeline=inference_pipeline,

        feature_columns=feature_cols,
    
        remove_columns=remove_columns,

        horizon=days_ahead,

        target=target,

    )

    ###############################################################
    # Intervalo de confiança (opcional)
    ###############################################################

    show_log("Calculando intervalos de confiança...")

    mean, lower, upper, std = prediction_interval_bootstrap(
        forecast=forecast,
        target=target,

    )

    ###############################################################
    # Decoder das categorias
    ###############################################################

    show_log("Decodificando categorias...")

    encoder = inference_pipeline.named_steps["category_encoder"]
    
    result= forecast.copy() 
    result= encoder.inverse_transform(result)
    

    ###############################################################
    # Intervalo de confiança
    ###############################################################
    result["mean"] = mean

    result["std"] = std

    result["lower_ci"] = lower

    result["upper_ci"] = upper

    show_log("=" * 70)
    show_log("Predição Finalizada")
    show_log(f"Linhas previstas: {len(result):,}")
    show_log("=" * 70)

    return result

# ---------------------------------------------------------------------------
# 9. Salvar no S3 Gold particionado
# ---------------------------------------------------------------------------


def save_to_s3_gold(
    df: pd.DataFrame,
    target: str = "quantidade",
    s3_gold_path: str = S3_GOLD_PATH,
) -> str:
    """
    Salva dataset no S3 Gold particionado por target/ano/mes/dia.
    Estrutura: Gold/<target>/YYYY/MM/DD/<target>_YYYY_MM_DD.parquet
    """
    s3_path = s3_gold_path.replace("s3://", "")
    bucket = s3_path.split("/")[0]
    base_prefix = "/".join(s3_path.split("/")[1:]).rstrip("/")

    s3_client = boto3.client("s3")
    _ensure_bucket(s3_client, bucket)

    df["data_venda"] = pd.to_datetime(df["data_venda"])
    df["data_prediction"]= datetime.now().strftime("%Y-%m-%d")
    date= datetime.now()
    year, month, day = date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")
    s3_key = f"{base_prefix}/forecast/{target}/{year}/{month}/{day}/{target}_{year}_{month}_{day}.parquet"

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=buf.getvalue())

    s3_uri = f"s3://{bucket}/{base_prefix}/{target}/"
    show_log(f"Salvo no S3 Gold: {s3_uri}, {len(df):,} registros)")
    return s3_uri


# ---------------------------------------------------------------------------
# 10. Avaliação das predições
# ---------------------------------------------------------------------------


def evaluate_predictions(
    df_consolidated: pd.DataFrame,
    target: str = "quantidade",
) -> Dict:
    """Avalia predições com estatísticas descritivas e comparação com histórico."""
    df_hist= df_consolidated[df_consolidated["tipo"] == "historico"]
    df_pred= df_consolidated[df_consolidated["tipo"] == "forecast"]

    hist_mean = df_hist[target].mean() if target in df_hist.columns else 0
    pred_mean = df_pred[target].mean() if not df_pred[target].empty else 0
    diff_pct = ((pred_mean - hist_mean) / hist_mean * 100) if hist_mean > 0 else 0


    show_log(f"\nAvaliação ({target}):")
    show_log(f"  Histórico → média: {hist_mean:.2f} | registros: {len(df_hist):,}")
    show_log(f"  Predições → média: {pred_mean:.2f} | registros: {len(df_pred):,}")
    show_log(f"  Variação vs histórico: {diff_pct:+.1f}%")
    if not df_pred[target].empty:
        show_log(f"  Predições → std: {df_pred[target].std():.2f} | min: {df_pred[target].min():.2f} | max: {df_pred[target].max():.2f}")

    return {
        "historico_mean": hist_mean,
        "predicao_mean": pred_mean,
        "variacao_pct": diff_pct,
        "n_historico": len(df_hist),
        "n_predicao": len(df_pred),
    }


# ---------------------------------------------------------------------------
# 11. Pipeline completo de inferência
# ---------------------------------------------------------------------------

def run_inference_pipeline(
    bucket: str = None,
    s3_gold_path: str = S3_GOLD_PATH,
    remove_columns: list= REMOVE_COLUMNS,
    days_ahead: int = 15,
    
) -> Dict:
    """
    Pipeline completo de inferência:
    1. Carrega modelos (quantidade e valor_total)
    2. Busca últimos 30 dias do Athena
    3. Preprocessa
    4. Gera features futuras
    5. Faz predições
    6. Monta datasets consolidados
    7. Salva no S3 Gold
    8. Avalia e plota

    Returns
    -------
    Dict
        Resultados com datasets, métricas e figuras.
    """
    show_log("=" * 60)
    show_log("GP Corp - Inference Pipeline")
    show_log(f"  Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    show_log(f"  Dias de predição: {days_ahead}")
    show_log("=" * 60)

    # 1. Carregar modelos
    show_log("\n1. Carregando modelos...")
    model_qty, preprocessing_pipeline_qty = model, preprocessing_pipeline=load_model_and_pipeline(
        model_name= "xgboost_quantidade",
        bucket= None,
        base_prefix= MODEL_S3_PREFIX,
        )
    
    model_val,preprocessing_pipeline_val = model, preprocessing_pipeline=load_model_and_pipeline(
        model_name= "xgboost_valor_total",
        bucket= None,
        base_prefix= MODEL_S3_PREFIX,
        )

    # 2. Buscar últimos 30 dias
    show_log("\n2. Buscando dados dos últimos 30 dias...")
    df_raw = fetch_last_days(database= ATHENA_DATABASE, days=90)

        
    # 3. Predições
    show_log("\n3. Fazendo predições...")
    df_pred_qty =  make_predictions(
        history=df_raw,
        model=model_qty,
        inference_pipeline=preprocessing_pipeline_qty,
        remove_columns=remove_columns,
        feature_cols=None,
        target="quantidade",
        days_ahead=days_ahead
        )
    
    df_pred_val = make_predictions(
        history=df_raw,
        model=model_val,
        inference_pipeline=preprocessing_pipeline_val,
        remove_columns=remove_columns,
        feature_cols=None,
        target="valor_total",
        days_ahead=days_ahead
        )
    

    # 4. Salvar no S3 Gold
    show_log("\n4. Salvando no S3 Gold...")
    s3_qty = save_to_s3_gold(df_pred_qty, target="quantidade", s3_gold_path=s3_gold_path)
    s3_val = save_to_s3_gold(df_pred_val, target="valor_total", s3_gold_path=s3_gold_path)

    # 5. Avaliação
    show_log("\n5. Avaliando predições...")
    eval_qty = evaluate_predictions(df_pred_qty, target="quantidade")
    eval_val = evaluate_predictions(df_pred_val, target="valor_total")

    show_log(f"Avaliação predições quantidade: /n{eval_qty}")
    show_log(f"\nAvaliação predições valor_total: /n{eval_val}")

    show_log("\n" + "=" * 60)
    show_log("Inferência concluída.")
    show_log(f"  Quantidade: s3 = {s3_qty}")
    show_log(f"  Valor Total: s3 = {s3_val}")
    show_log("=" * 60)

    return {
        "df_consolidated_qty": df_pred_qty,
        "df_consolidated_val": df_pred_val,
        "eval_qty": eval_qty,
        "eval_val": eval_val,
        "s3_paths": {"quantidade": s3_qty, "valor_total": s3_val},
    }

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    results = run_inference_pipeline()
