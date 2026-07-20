"""src
SageMaker - Treinamento do Modelo XGBoost para Série Temporal (Previsão de Vendas)

Etapas:
1. Carrega dados preprocessados (train/val)
2. Grid Search para encontrar melhores hiperparâmetros
3. Treina modelo final com melhores parâmetros
4. Salva modelo em formato .pkl
5. Registra modelo no SageMaker para predições futuras
6. Feature importance

Uso no notebook:
    from sagemaker_training import run_training_pipeline
    model, results = run_training_pipeline(df_train, df_val, target="quantidade")
"""

import os
import json
import joblib
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from xgboost import XGBRegressor

from sagemaker_logs import show_log

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

MODEL_DIR = os.environ.get("MODEL_DIR", "/opt/ml/model")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/ml/processing/output")

# Paleta GP Corp
GPCORP_COLORS = {
    "azul_escuro": "#1B3A5C",
    "azul_medio": "#2E6B9E",
    "azul_claro": "#5BA4D9",
    "laranja": "#E8772E",
    "laranja_claro": "#F5A623",
    "dourado": "#D4A843",
    "cinza_escuro": "#3D3D3D",
    "branco": "#FFFFFF",
}

GPCORP_SEQUENCE = [
    GPCORP_COLORS["azul_escuro"],
    GPCORP_COLORS["laranja"],
    GPCORP_COLORS["azul_medio"],
    GPCORP_COLORS["dourado"],
    GPCORP_COLORS["azul_claro"],
    GPCORP_COLORS["laranja_claro"],
    GPCORP_COLORS["cinza_escuro"],
]

# Colunas que NÃO devem ser usadas como features
EXCLUDE_COLS = [
    "data_venda", "item_code", "item_name", "cliente_code",
    "cliente_nome", "vendedor_nome",
]

# Hiperparâmetros para Grid Search
DEFAULT_PARAM_GRID = {
    "xgb__n_estimators": [100, 300, 500],
    "xgb__max_depth": [3, 5, 7, 9],
    "xgb__learning_rate": [0.01, 0.05, 0.1],
    "xgb__subsample": [0.7, 0.8, 1.0],
    "xgb__colsample_bytree": [0.7, 0.8, 1.0],
    "xgb__min_child_weight": [1, 3, 5],
}

# Grid reduzido para testes rápidos
FAST_PARAM_GRID = {
    "xgb__n_estimators": [100, 300],
    "xgb__max_depth": [3, 5, 7],
    "xgb__learning_rate": [0.05, 0.1],
    "xgb__subsample": [0.8],
    "xgb__colsample_bytree": [0.8],
    "xgb__min_child_weight": [1, 3],
}


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------


def _apply_gpcorp_style(fig, title: str = "", height: int = 500):
    """Aplica estilo GP Corp ao gráfico."""
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


def get_feature_columns(df: pd.DataFrame, target: str) -> List[str]:
    """
    Retorna lista de colunas de features (exclui target, IDs e não-numéricas restantes).
    """
    all_cols = df.columns.tolist()
    exclude = [target]
    feature_cols = [c for c in all_cols if c not in exclude and df[c].dtype in ["int64", "float64", "int32", "float32", "uint8"]]
    return feature_cols


def prepare_xy(df: pd.DataFrame, target: str, feature_cols: List[str] = None) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Separa features (X) e target (y) do DataFrame.
    """
    if feature_cols is None:
        feature_cols = get_feature_columns(df, target)

    X = df[feature_cols].copy()
    y = df[target].copy()
    y=np.log1p(y)

    return X, y

# ---------------------------------------------------------------------------
# Grid Search
# ---------------------------------------------------------------------------


def run_grid_search(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    param_grid: Dict = None,
    n_splits: int = 3,
    scoring: str = "neg_mean_absolute_error",
    n_jobs: int = -1,
    fast: bool = False,
    use_randomized: bool = True,
    n_iter: int = 20,
    subsample_fraction: float = None,
) -> Tuple[object, Dict]:
    """
    Executa Grid Search ou Randomized Search com TimeSeriesSplit.

    Parameters
    ----------
    X_train : pd.DataFrame
        Features de treino.
    y_train : pd.Series
        Target de treino.
    param_grid : dict, optional
        Grade de hiperparâmetros.
    n_splits : int
        Número de folds do TimeSeriesSplit (3 é suficiente para séries temporais).
    scoring : str
        Métrica de otimização.
    n_jobs : int
        Número de jobs paralelos (-1 = todos os cores).
    fast : bool
        Se True, usa grid reduzido.
    use_randomized : bool
        Se True, usa RandomizedSearchCV (mais rápido). Se False, usa GridSearchCV.
    n_iter : int
        Número de combinações aleatórias (só para RandomizedSearchCV).
    subsample_fraction : float, optional
        Fração do dataset para o search (ex: 0.3 = usa 30% para buscar params).
        O modelo final é treinado com 100% dos dados.

    Returns
    -------
    Tuple[object, Dict]
        (search_fitted, best_params)
    """
    if param_grid is None:
        param_grid = FAST_PARAM_GRID if fast else DEFAULT_PARAM_GRID

    # Subsampling para acelerar a busca
    if subsample_fraction and subsample_fraction < 1.0:
        n_sample = int(len(X_train) * subsample_fraction)
        # Pegar últimas N amostras (respeitar ordem temporal)
        X_search = X_train.iloc[-n_sample:]
        y_search = y_train.iloc[-n_sample:]
        show_log(f"  Subsample para search: {n_sample:,} amostras ({subsample_fraction*100:.0f}% do treino)")
    else:
        X_search = X_train
        y_search = y_train

    total_combinations = 1
    for v in param_grid.values():
        total_combinations *= len(v)

    show_log("=" * 60)
    show_log(f"Iniciando {'Randomized' if use_randomized else 'Grid'} Search com TimeSeriesSplit")
    show_log(f"  Folds: {n_splits}")
    show_log(f"  Scoring: {scoring}")
    show_log(f"  Combinações totais na grade: {total_combinations}")
    if use_randomized:
        actual_iter = min(n_iter, total_combinations)
        show_log(f"  Iterações (randomized): {actual_iter}")
        show_log(f"  Fits totais: {actual_iter * n_splits}")
    else:
        show_log(f"  Fits totais: {total_combinations * n_splits}")
    show_log(f"  Amostras para search: {len(X_search):,}")
    show_log(f"  n_jobs: {n_jobs}")
    show_log("=" * 60)

    # Pipeline com XGBoost
    pipeline = Pipeline([
        ("xgb", XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            verbosity=0,
            tree_method="hist",  # Mais rápido para datasets grandes
            n_jobs=1,  # Evitar conflito com n_jobs do CV
        )),
    ])

    # TimeSeriesSplit
    tscv = TimeSeriesSplit(n_splits=n_splits)

    if use_randomized:
        search = RandomizedSearchCV(
            estimator=pipeline,
            param_distributions=param_grid,
            n_iter=min(n_iter, total_combinations),
            cv=tscv,
            scoring=scoring,
            n_jobs=n_jobs,
            verbose=1,
            refit=True,
            random_state=42,
        )
    else:
        search = GridSearchCV(
            estimator=pipeline,
            param_grid=param_grid,
            cv=tscv,
            scoring=scoring,
            n_jobs=n_jobs,
            verbose=1,
            refit=True,
        )

    start_time = datetime.now()
    search.fit(X_search, y_search)
    elapsed = (datetime.now() - start_time).total_seconds()

    best_params = search.best_params_
    best_score = search.best_score_

    show_log(f"\nSearch concluído em {elapsed:.1f}s ({elapsed/60:.1f} min)")
    show_log(f"Melhor score ({scoring}): {best_score:.4f}")
    show_log(f"Melhores parâmetros:")
    for param, value in best_params.items():
        show_log(f"  {param}: {value}")

    return search, best_params


# ---------------------------------------------------------------------------
# Treinamento do modelo final
# ---------------------------------------------------------------------------


def train_final_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    best_params: Dict,
) -> Pipeline:
    """
    Treina o modelo final com os melhores parâmetros encontrados no Grid Search.

    Parameters
    ----------
    X_train : pd.DataFrame
        Features de treino.
    y_train : pd.Series
        Target de treino.
    best_params : dict
        Melhores hiperparâmetros (do grid search).

    Returns
    -------
    Pipeline
        Pipeline treinado com XGBoost.
    """
    # Extrair parâmetros do XGBoost (remover prefixo 'xgb__')
    xgb_params = {k.replace("xgb__", ""): v for k, v in best_params.items()}

    show_log("\nTreinando modelo final com melhores parâmetros...")

    pipeline = Pipeline([
        ("xgb", XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            verbosity=0,
            **xgb_params,
        )),
    ])

    pipeline.fit(X_train, y_train)

    show_log(f"Modelo treinado com {len(X_train):,} amostras e {X_train.shape[1]} features.")
    return pipeline


# ---------------------------------------------------------------------------
# Feature Importance
# ---------------------------------------------------------------------------


def get_feature_importance(model_pipeline: Pipeline, feature_names: List[str]) -> pd.DataFrame:
    """
    Extrai feature importance do modelo XGBoost.

    Parameters
    ----------
    model_pipeline : Pipeline
        Pipeline treinado.
    feature_names : list
        Nomes das features.

    Returns
    -------
    pd.DataFrame
        DataFrame com colunas ['feature', 'importance'] ordenado por importância.
    """
    xgb_model = model_pipeline.named_steps["xgb"]
    importances = xgb_model.feature_importances_

    df_importance = pd.DataFrame({
        "feature": feature_names,
        "importance": importances,
    }).sort_values("importance", ascending=False).reset_index(drop=True)

    show_log(f"\nTop 10 features mais importantes:")
    for _, row in df_importance.head(10).iterrows():
        show_log(f"  {row['feature']}: {row['importance']:.4f}")

    return df_importance


def plot_feature_importance(
    df_importance: pd.DataFrame,
    top_n: int = 20,
    title: str = "Feature Importance — XGBoost",
):
    """
    Gráfico interativo de feature importance.

    Parameters
    ----------
    df_importance : pd.DataFrame
        DataFrame com colunas ['feature', 'importance'].
    top_n : int
        Número de features a exibir.
    title : str
        Título do gráfico.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    df_top = df_importance.head(top_n).sort_values("importance", ascending=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_top["importance"],
        y=df_top["feature"],
        orientation="h",
        marker_color=GPCORP_COLORS["azul_medio"],
        text=[f"{v:.4f}" for v in df_top["importance"]],
        textposition="outside",
    ))

    _apply_gpcorp_style(fig, title=title, height=max(400, top_n * 25))
    fig.update_xaxes(title_text="Importance")
    fig.update_yaxes(title_text="Feature")
    fig.show()
    return fig


# ---------------------------------------------------------------------------
# Salvar e registrar modelo
# ---------------------------------------------------------------------------


def save_model(
    model_pipeline: Pipeline,
    model_dir: str = MODEL_DIR,
    filename: str = "model_xgboost_vendas.pkl",
    metadata: Dict = None,
    preprocessing_pipeline=None,
) -> str:
    """
    Salva o modelo treinado em formato .pkl.
    Opcionalmente salva o pipeline de preprocessamento junto.

    Parameters
    ----------
    model_pipeline : Pipeline
        Pipeline treinado.
    model_dir : str
        Diretório de saída.
    filename : str
        Nome do arquivo.
    metadata : dict, optional
        Metadados adicionais para salvar junto (parâmetros, métricas, etc.)
    preprocessing_pipeline : Pipeline, optional
        Pipeline de preprocessamento (NullHandler, Temporal, ZScore, Dummifier).

    Returns
    -------
    str
        Caminho do arquivo salvo.
    """
    os.makedirs(model_dir, exist_ok=True)
    filepath = os.path.join(model_dir, filename)

    joblib.dump(model_pipeline, filepath)
    show_log(f"Modelo salvo em: {filepath}")

    # Salvar pipeline de preprocessamento
    if preprocessing_pipeline is not None:
        preproc_path = os.path.join(model_dir, "preprocessing_pipeline.pkl")
        joblib.dump(preprocessing_pipeline, preproc_path)
        show_log(f"Pipeline de preprocessamento salvo em: {preproc_path}")

    # Salvar metadados
    if metadata:
        metadata_path = os.path.join(model_dir, "model_metadata.json")
        # Converter tipos numpy para JSON
        metadata_serializable = {
            k: (v.item() if hasattr(v, "item") else v)
            for k, v in metadata.items()
        }
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata_serializable, f, indent=2, ensure_ascii=False, default=str)
        show_log(f"Metadados salvos em: {metadata_path}")

    return filepath


def load_model(model_path: str) -> Pipeline:
    """Carrega modelo salvo em .pkl."""
    model = joblib.load(model_path)
    show_log(f"Modelo carregado de: {model_path}")
    return model


def register_model_sagemaker(
    model_path: str,
    model_name: str = "gpcorp-xgboost-vendas",
    bucket: str = None,
    prefix: str = "time-series/xgboost",
):
    """
    Registra o modelo no SageMaker Model Registry via upload para S3.
    Usa o bucket padrão do SageMaker se nenhum bucket for informado.

    Parameters
    ----------
    model_path : str
        Caminho local do modelo .pkl.
    model_name : str
        Nome do modelo no registry.
    bucket : str, optional
        Bucket S3. Se None, usa o bucket padrão do SageMaker.
    prefix : str
        Prefixo no bucket.

    Returns
    -------
    str
        URI S3 do modelo.
    """
    import boto3
    import tarfile
    import tempfile

    # Usar bucket padrão do SageMaker se não informado
    if bucket is None:
        sts = boto3.client("sts")
        account_id = sts.get_caller_identity()["Account"]
        region = boto3.session.Session().region_name or "us-east-1"
        bucket = f"sagemaker-{region}-{account_id}"
        show_log(f"Usando bucket padrão do SageMaker: {bucket}")

    # Criar bucket se não existir
    s3_client = boto3.client("s3")
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
        show_log(f"Bucket '{bucket}' criado com sucesso.")

    # Criar tarball (formato esperado pelo SageMaker)
    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tar_path = tmp.name

    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(model_path, arcname=os.path.basename(model_path))
        # Incluir metadados se existirem
        metadata_path = os.path.join(os.path.dirname(model_path), "model_metadata.json")
        if os.path.exists(metadata_path):
            tar.add(metadata_path, arcname="model_metadata.json")

    # Upload para S3
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{prefix}/{model_name}/{timestamp}/model.tar.gz"
    s3_uri = f"s3://{bucket}/{s3_key}"

    s3_client = boto3.client("s3")
    s3_client.upload_file(tar_path, bucket, s3_key)

    show_log(f"Modelo registrado no S3: {s3_uri}")

    # Limpar arquivo temporário
    os.remove(tar_path)

    return s3_uri


# ---------------------------------------------------------------------------
# Pipeline completo de treinamento
# ---------------------------------------------------------------------------


def run_training_pipeline(
    df_train: pd.DataFrame,
    df_val: pd.DataFrame,
    target: str = "quantidade",
    fast_search: bool = True,
    param_grid: Dict = None,
    use_randomized: bool = True,
    n_iter: int = 20,
    subsample_fraction: float = None,
    n_splits: int = 3,
    n_jobs: int = -1,
    save: bool = True,
    model_dir: str = MODEL_DIR,
    register_s3: bool = False,
    preprocessing_pipeline=None,
) -> Tuple[Pipeline, Dict]:
    """
    Pipeline completo de treinamento:
    1. Prepara X, y
    2. Grid/Randomized Search (melhores hiperparâmetros)
    3. Treina modelo final
    4. Feature importance
    5. Salva modelo
    6. (Opcional) Registra no SageMaker/S3

    Parameters
    ----------
    df_train : pd.DataFrame
        Dados de treino (já preprocessados).
    df_val : pd.DataFrame
        Dados de validação.
    target : str
        Coluna target ('quantidade' ou 'lucro_bruto').
    fast_search : bool
        Se True, usa grid reduzido.
    param_grid : dict, optional
        Grid customizado.
    use_randomized : bool
        Se True, usa RandomizedSearchCV (recomendado para grandes datasets).
    n_iter : int
        Iterações do RandomizedSearch.
    subsample_fraction : float, optional
        Fração do dataset para busca de params (ex: 0.3 usa 30%).
    n_splits : int
        Folds do TimeSeriesSplit.
    n_jobs : int
        Paralelismo.
    save : bool
        Se True, salva modelo em .pkl.
    model_dir : str
        Diretório para salvar modelo.
    register_s3 : bool
        Se True, faz upload do modelo para S3.
    preprocessing_pipeline : Pipeline, optional
        Pipeline de preprocessamento fitted para salvar junto ao modelo.

    Returns
    -------
    Tuple[Pipeline, Dict]
        (modelo_treinado, resultados)
    """
    show_log("=" * 60)
    show_log("GP Corp - Training Pipeline (XGBoost)")
    show_log(f"Target: {target}")
    show_log("=" * 60)

    # 1. Preparar features
    feature_cols = get_feature_columns(df_train, target)
    show_log(f"\nFeatures selecionadas: {len(feature_cols)}")

    X_train, y_train = prepare_xy(df_train, target, feature_cols)
    X_val, y_val = prepare_xy(df_val, target, feature_cols)

    show_log(f"Treino: {X_train.shape[0]:,} amostras")
    show_log(f"Validação: {X_val.shape[0]:,} amostras")

    # 2. Grid Search
    grid = param_grid if param_grid else (FAST_PARAM_GRID if fast_search else DEFAULT_PARAM_GRID)
    grid_search, best_params = run_grid_search(
        X_train, y_train,
        param_grid=grid,
        n_splits=n_splits,
        n_jobs=n_jobs,
        use_randomized=use_randomized,
        n_iter=n_iter,
        subsample_fraction=subsample_fraction,
    )

    # 3. Modelo final (refit do grid search já treina com melhores params)
    model_pipeline = grid_search.best_estimator_

    # 4. Predições na validação
    y_pred_val = model_pipeline.predict(X_val)
    y_pred_val = np.expm1(y_pred_val)
    y_val = np.expm1(y_val)
    from sagemaker_evaluate import calculate_metrics
    val_metrics = calculate_metrics(y_val.values, y_pred_val)
    show_log(f"\nMétricas na Validação:")
    for metric, value in val_metrics.items():
        show_log(f"  {metric}: {value:.4f}")

    # 5. Feature importance
    df_importance = get_feature_importance(model_pipeline, feature_cols)
    fig_importance = plot_feature_importance(df_importance)

    # 6. Salvar
    results = {
        "best_params": best_params,
        "val_metrics": val_metrics,
        "feature_importance": df_importance,
        "feature_cols": feature_cols,
        "target": target,
        "train_size": len(X_train),
        "val_size": len(X_val),
        "timestamp": datetime.now().isoformat(),
    }

    model_path = None
    if save:
        metadata = {
            "target": target,
            "best_params": best_params,
            "val_metrics": val_metrics,
            "n_features": len(feature_cols),
            "feature_cols": feature_cols,
            "train_size": len(X_train),
            "timestamp": datetime.now().isoformat(),
        }
        model_path = save_model(
            model_pipeline, model_dir=model_dir, metadata=metadata,
            preprocessing_pipeline=preprocessing_pipeline,
        )
        results["model_path"] = model_path

    if register_s3 and model_path:
        s3_uri = register_model_sagemaker(model_path)
        results["s3_uri"] = s3_uri

    show_log("\nTreinamento concluído.")
    show_log("=" * 60)

    return model_pipeline, results


# ---------------------------------------------------------------------------
# Registrar modelo no S3 com estrutura ano/mes/dia
# ---------------------------------------------------------------------------


def register_model_s3_structured(
    model_path: str,
    model_name: str,
    bucket: str = None,
    base_prefix: str = "models/time-series",
    preprocessing_pipeline=None,
) -> str:
    """
    Registra o modelo e o pipeline de preprocessamento no S3.

    Estrutura:
    <base_prefix>/<model_name>/YYYY/MM/DD/<model_name>_YYYYMMDD_HHmmss.pkl
    <base_prefix>/<model_name>/YYYY/MM/DD/preprocessing_pipeline.pkl
    """
    import boto3

    # Bucket padrão SageMaker se não informado
    if bucket is None:
        sts = boto3.client("sts")
        account_id = sts.get_caller_identity()["Account"]
        region = boto3.session.Session().region_name or "us-east-1"
        bucket = f"sagemaker-{region}-{account_id}"

    # Criar bucket se não existir
    s3_client = boto3.client("s3")
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
        show_log(f"Bucket '{bucket}' criado com sucesso.")

    now = datetime.now()
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    model_dir = os.path.dirname(model_path)

    # 1. Upload do modelo
    s3_key = f"{base_prefix}/{model_name}/{year}/{month}/{day}/{model_name}_{timestamp}.pkl"
    s3_uri = f"s3://{bucket}/{s3_key}"
    s3_client.upload_file(model_path, bucket, s3_key)
    show_log(f"Modelo registrado no S3: {s3_uri}")

    # 2. Upload dos metadados
    metadata_path = os.path.join(model_dir, "model_metadata.json")
    if os.path.exists(metadata_path):
        meta_key = f"{base_prefix}/{model_name}/{year}/{month}/{day}/{model_name}_{timestamp}_metadata.json"
        s3_client.upload_file(metadata_path, bucket, meta_key)
        show_log(f"Metadados registrados no S3: s3://{bucket}/{meta_key}")

    # 3. Salvar e fazer upload do pipeline de preprocessamento
    if preprocessing_pipeline is not None:
        preproc_local = os.path.join(model_dir, "preprocessing_pipeline.pkl")
        joblib.dump(preprocessing_pipeline, preproc_local)
        show_log(f"Pipeline de preprocessamento salvo localmente: {preproc_local}")

        preproc_key = f"{base_prefix}/{model_name}/{year}/{month}/{day}/preprocessing_pipeline.pkl"
        s3_client.upload_file(preproc_local, bucket, preproc_key)
        show_log(f"Pipeline de preprocessamento registrado no S3: s3://{bucket}/{preproc_key}")

    return s3_uri


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def training_models(train_path, test_path, val_path, model_path, preprocessing_pipeline, fast=True, n_iter=20, n_splits=3, n_jobs=2, subsample=None, buket=None, save=True, register_s3=True):
    """
    Execução principal: treina e salva modelos de predição para:
    1. Quantidade vendida
    2. Valor total da venda

    Salva os modelos localmente em .pkl e no S3 com estrutura:
    models/time-series/<model_name>/YYYY/MM/DD/<model_name>_YYYYMMDD_HHmmss.pkl
    """

    df_train = pd.read_parquet(train_path)
    df_val = pd.read_parquet(val_path)

    show_log("=" * 60)
    show_log("GP Corp - Treinamento de Modelos de Série Temporal")
    show_log(f"  Registros de treino: {len(df_train):,}")
    show_log(f"  Registros de validação: {len(df_val):,}")
    show_log("=" * 60)

    # Targets para treinar
    targets = [
    {"target": "quantidade",  "model_name": "xgboost_quantidade"},
    {"target": "valor_total", "model_name": "xgboost_valor_total"},
    ]

    results_all = {}
    
    for target_config in targets:
        target     = target_config["target"]
        model_name = target_config["model_name"]
    
        model_pipeline, results = run_training_pipeline(
            df_train=df_train,
            df_val=df_val,
            target=target,
            fast_search=fast,
            use_randomized=True,
            n_iter=n_iter,
            n_splits=n_splits,
            n_jobs=n_jobs,
            save=save,
            model_dir=os.path.join(model_path, model_name),   # <-- diretório separado por modelo
            register_s3=register_s3,
            preprocessing_pipeline=preprocessing_pipeline,
        )
    
        model_path_ = results.get("model_path")
        if model_path_:
            s3_uri = register_model_s3_structured(
                model_path=model_path_,
                model_name=model_name,
                bucket=None,
                preprocessing_pipeline=preprocessing_pipeline,
            )
            results["s3_uri"] = s3_uri
    
        results_all[target] = results

    # Resumo final
    show_log(f"\n{'=' * 60}")
    show_log("RESUMO DO TREINAMENTO")
    show_log(f"{'=' * 60}")
    for target, results in results_all.items():
        show_log(f"\n  Modelo: {target}")
        show_log(f"    MAE:  {results['val_metrics']['MAE']:.4f}")
        show_log(f"    RMSE: {results['val_metrics']['RMSE']:.4f}")
        show_log(f"    MAPE: {results['val_metrics']['MAPE']:.2f}%")
        show_log(f"    S3:   {results.get('s3_uri', 'N/A')}")

    show_log(f"\n{'=' * 60}")
    show_log("Treinamento de todos os modelos concluído.")
    show_log(f"{'=' * 60}")

def main():
    """
    Execução principal: treina e salva modelos de predição para:
    1. Quantidade vendida
    2. Valor total da venda

    Salva os modelos localmente em .pkl e no S3 com estrutura:
    models/time-series/<model_name>/YYYY/MM/DD/<model_name>_YYYYMMDD_HHmmss.pkl
    """
    import argparse

    parser = argparse.ArgumentParser(description="XGBoost Training Pipeline - Quantidade e Valor Total")
    parser.add_argument("--train-path", type=str, required=True, help="Caminho do parquet de treino")
    parser.add_argument("--val-path", type=str, required=True, help="Caminho do parquet de validação")
    parser.add_argument("--model-dir", type=str, default=MODEL_DIR, help="Diretório local para salvar modelos")
    parser.add_argument("--fast", action="store_true", default=True, help="Grid search rápido")
    parser.add_argument("--n-iter", type=int, default=20, help="Iterações do RandomizedSearch")
    parser.add_argument("--n-jobs", type=int, default=4, help="Paralelismo")
    parser.add_argument("--subsample", type=float, default=None, help="Fração para search (ex: 0.3)")
    parser.add_argument("--bucket", type=str, default=None, help="Bucket S3 (None = padrão SageMaker)")
    args = parser.parse_args()

    df_train = pd.read_parquet(args.train_path)
    df_val = pd.read_parquet(args.val_path)

    show_log("=" * 60)
    show_log("GP Corp - Treinamento de Modelos de Série Temporal")
    show_log(f"  Registros de treino: {len(df_train):,}")
    show_log(f"  Registros de validação: {len(df_val):,}")
    show_log("=" * 60)

    # Targets para treinar
    targets = [
    {"target": "quantidade",  "model_name": "xgboost_quantidade"},
    {"target": "valor_total", "model_name": "xgboost_valor_total"},
    ]

    results_all = {}
    
    for target_config in targets:
        target     = target_config["target"]
        model_name = target_config["model_name"]
    
        model_pipeline, results = run_training_pipeline(
            df_train=df_train,
            df_val=df_val,
            target=target,
            fast_search=True,
            use_randomized=True,
            n_iter=20,
            n_splits=3,
            n_jobs=2,
            save=True,
            model_dir=f"./models/{model_name}",   # <-- diretório separado por modelo
            register_s3=False,
            preprocessing_pipeline=preprocessing_pipeline,
        )
    
        model_path = results.get("model_path")
        if model_path:
            s3_uri = register_model_s3_structured(
                model_path=model_path,
                model_name=model_name,
                bucket=None,
                preprocessing_pipeline=preprocessing_pipeline,
            )
            results["s3_uri"] = s3_uri
    
        results_all[target] = results
    # Resumo final
    show_log(f"\n{'=' * 60}")
    show_log("RESUMO DO TREINAMENTO")
    show_log(f"{'=' * 60}")
    for target, results in results_all.items():
        show_log(f"\n  Modelo: {target}")
        show_log(f"    MAE:  {results['val_metrics']['MAE']:.4f}")
        show_log(f"    RMSE: {results['val_metrics']['RMSE']:.4f}")
        show_log(f"    MAPE: {results['val_metrics']['MAPE']:.2f}%")
        show_log(f"    S3:   {results.get('s3_uri', 'N/A')}")

    show_log(f"\n{'=' * 60}")
    show_log("Treinamento de todos os modelos concluído.")
    show_log(f"{'=' * 60}")


if __name__ == "__main__":
    main()
