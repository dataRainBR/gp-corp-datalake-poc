"""
SageMaker - Avaliação do Modelo XGBoost (Série Temporal - Previsão de Vendas)

Métricas de avaliação:
- MAE (Mean Absolute Error)
- RMSE (Root Mean Squared Error)
- MAPE (Mean Absolute Percentage Error)

Inclui visualizações de predito vs real, resíduos e análise de erro por período.

Uso no notebook:
    from sagemaker_evaluate import evaluate_model, plot_predictions_vs_actual
    metrics = evaluate_model(model, X_test, y_test)
"""

import os
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.pipeline import Pipeline

from sagemaker_logs import show_log

# ---------------------------------------------------------------------------
# Paleta GP Corp
# ---------------------------------------------------------------------------

GPCORP_COLORS = {
    "azul_escuro": "#1B3A5C",
    "azul_medio": "#2E6B9E",
    "azul_claro": "#5BA4D9",
    "laranja": "#E8772E",
    "laranja_claro": "#F5A623",
    "dourado": "#D4A843",
    "cinza_escuro": "#3D3D3D",
    "cinza_medio": "#6B6B6B",
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
    GPCORP_COLORS["cinza_escuro"],
]


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


# ---------------------------------------------------------------------------
# Métricas
# ---------------------------------------------------------------------------


def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean Absolute Error."""
    return np.mean(np.abs(y_true - y_pred))


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Root Mean Squared Error."""
    return np.sqrt(np.mean((y_true - y_pred) ** 2))


def mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Mean Absolute Percentage Error.
    Ignora valores onde y_true == 0 para evitar divisão por zero.
    """
    mask = y_true != 0
    if mask.sum() == 0:
        return np.nan
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100


def calculate_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    """
    Calcula todas as métricas de avaliação.

    Parameters
    ----------
    y_true : np.ndarray
        Valores reais.
    y_pred : np.ndarray
        Valores preditos.

    Returns
    -------
    Dict[str, float]
        Dicionário com MAE, RMSE, MAPE.
    """
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)

    metrics = {
        "MAE": mae(y_true, y_pred),
        "RMSE": rmse(y_true, y_pred),
        "MAPE": mape(y_true, y_pred),
    }
    return metrics


# ---------------------------------------------------------------------------
# Avaliação completa
# ---------------------------------------------------------------------------


def evaluate_model(
    model: Pipeline,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    dataset_name: str = "Teste",
) -> Dict[str, float]:
    """
    Avalia o modelo e exibe métricas formatadas.

    Parameters
    ----------
    model : Pipeline
        Modelo treinado.
    X_test : pd.DataFrame
        Features de teste.
    y_test : pd.Series
        Target real.
    dataset_name : str
        Nome do dataset (para log).

    Returns
    -------
    Dict[str, float]
        Métricas calculadas.
    """
    y_pred = model.predict(X_test)
    metrics = calculate_metrics(y_test.values, y_pred)

    show_log(f"\n{'=' * 40}")
    show_log(f"Avaliação do Modelo — {dataset_name}")
    show_log(f"{'=' * 40}")
    show_log(f"  MAE:  {metrics['MAE']:.4f}")
    show_log(f"  RMSE: {metrics['RMSE']:.4f}")
    show_log(f"  MAPE: {metrics['MAPE']:.2f}%")
    show_log(f"  Amostras: {len(y_test):,}")
    show_log(f"{'=' * 40}")

    return metrics


def evaluate_all_splits(
    model: Pipeline,
    X_train: pd.DataFrame, y_train: pd.Series,
    X_val: pd.DataFrame, y_val: pd.Series,
    X_test: pd.DataFrame, y_test: pd.Series,
) -> Dict[str, Dict[str, float]]:
    """
    Avalia o modelo em todos os splits (treino, validação, teste).

    Returns
    -------
    Dict com métricas para cada split.
    """
    results = {}

    show_log("\n" + "=" * 60)
    show_log("AVALIAÇÃO COMPLETA DO MODELO")
    show_log("=" * 60)

    results["train"] = evaluate_model(model, X_train, y_train, "Treino")
    results["val"] = evaluate_model(model, X_val, y_val, "Validação")
    results["test"] = evaluate_model(model, X_test, y_test, "Teste")

    # Detectar overfitting
    train_mae = results["train"]["MAE"]
    test_mae = results["test"]["MAE"]
    ratio = test_mae / train_mae if train_mae > 0 else 0

    if ratio > 2.0:
        show_log(f"\n⚠️  Possível OVERFITTING detectado: MAE teste/treino = {ratio:.2f}")
    elif ratio < 1.2:
        show_log(f"\n✓  Modelo generaliza bem: MAE teste/treino = {ratio:.2f}")
    else:
        show_log(f"\n  MAE teste/treino = {ratio:.2f} (aceitável)")

    return results


# ---------------------------------------------------------------------------
# Visualizações
# ---------------------------------------------------------------------------


def plot_predictions_vs_actual(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    title: str = "Predito vs Real",
    n_samples: int = 500,
):
    """
    Scatter plot de valores preditos vs reais + linha de referência perfeita.
    """
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)

    # Amostrar se muito grande
    if len(y_true) > n_samples:
        idx = np.random.choice(len(y_true), n_samples, replace=False)
        y_true_plot = y_true[idx]
        y_pred_plot = y_pred[idx]
    else:
        y_true_plot = y_true
        y_pred_plot = y_pred

    fig = go.Figure()

    # Scatter
    fig.add_trace(go.Scatter(
        x=y_true_plot, y=y_pred_plot,
        mode="markers",
        marker=dict(color=GPCORP_COLORS["azul_medio"], size=5, opacity=0.5),
        name="Predições",
    ))

    # Linha perfeita (y = x)
    min_val = min(y_true_plot.min(), y_pred_plot.min())
    max_val = max(y_true_plot.max(), y_pred_plot.max())
    fig.add_trace(go.Scatter(
        x=[min_val, max_val], y=[min_val, max_val],
        mode="lines",
        line=dict(color=GPCORP_COLORS["laranja"], width=2, dash="dash"),
        name="Perfeito (y=x)",
    ))

    _apply_gpcorp_style(fig, title=title)
    fig.update_xaxes(title_text="Valor Real")
    fig.update_yaxes(title_text="Valor Predito")
    fig.show()
    return fig


def plot_residuals(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    title: str = "Análise de Resíduos",
):
    """
    Gráfico de resíduos: distribuição e resíduo vs predito.
    """
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    residuals = y_true - y_pred

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=["Distribuição dos Resíduos", "Resíduos vs Predito"],
    )

    # Histograma dos resíduos
    fig.add_trace(
        go.Histogram(
            x=residuals, nbinsx=50,
            marker_color=GPCORP_COLORS["azul_medio"],
            opacity=0.8,
            name="Resíduos",
        ),
        row=1, col=1,
    )

    # Resíduo vs predito
    n_plot = min(len(y_pred), 1000)
    idx = np.random.choice(len(y_pred), n_plot, replace=False) if len(y_pred) > n_plot else np.arange(len(y_pred))
    fig.add_trace(
        go.Scatter(
            x=y_pred[idx], y=residuals[idx],
            mode="markers",
            marker=dict(color=GPCORP_COLORS["laranja"], size=4, opacity=0.4),
            name="Resíduos",
        ),
        row=1, col=2,
    )

    # Linha zero
    fig.add_hline(y=0, line_dash="dash", line_color=GPCORP_COLORS["cinza_escuro"], row=1, col=2)

    _apply_gpcorp_style(fig, title=title, height=400)
    fig.show()
    return fig


def plot_error_over_time(
    df_test: pd.DataFrame,
    y_pred: np.ndarray,
    target: str = "quantidade",
    freq: str = "W",
):
    """
    Evolução do erro (MAE) ao longo do tempo.
    Permite identificar períodos com pior performance.
    """
    df_temp = df_test.copy()
    df_temp["y_pred"] = y_pred
    df_temp["error"] = np.abs(df_temp[target] - df_temp["y_pred"])
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])

    ts_error = df_temp.set_index("data_venda")["error"].resample(freq).mean().reset_index()
    ts_error.columns = ["data_venda", "mae"]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ts_error["data_venda"], y=ts_error["mae"],
        mode="lines",
        line=dict(color=GPCORP_COLORS["laranja"], width=2),
        name="MAE",
    ))

    # Linha de MAE geral
    mae_geral = ts_error["mae"].mean()
    fig.add_hline(
        y=mae_geral, line_dash="dash",
        line_color=GPCORP_COLORS["azul_escuro"],
        annotation_text=f"MAE médio: {mae_geral:.2f}",
    )

    _apply_gpcorp_style(fig, title=f"Erro (MAE) ao Longo do Tempo ({freq})")
    fig.update_xaxes(title_text="Data")
    fig.update_yaxes(title_text="MAE")
    fig.show()
    return fig


def plot_predictions_timeline(
    df_test: pd.DataFrame,
    y_pred: np.ndarray,
    target: str = "quantidade",
    freq: str = "W",
):
    """
    Comparação temporal: real vs predito agregado por período.
    """
    df_temp = df_test.copy()
    df_temp["y_pred"] = y_pred
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])

    ts_real = df_temp.set_index("data_venda")[target].resample(freq).sum().reset_index()
    ts_pred = df_temp.set_index("data_venda")["y_pred"].resample(freq).sum().reset_index()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ts_real["data_venda"], y=ts_real[target],
        mode="lines",
        line=dict(color=GPCORP_COLORS["azul_escuro"], width=2),
        name="Real",
    ))
    fig.add_trace(go.Scatter(
        x=ts_pred["data_venda"], y=ts_pred["y_pred"],
        mode="lines",
        line=dict(color=GPCORP_COLORS["laranja"], width=2, dash="dot"),
        name="Predito",
    ))

    _apply_gpcorp_style(fig, title=f"Real vs Predito: {target} ({freq})")
    fig.update_xaxes(title_text="Data")
    fig.update_yaxes(title_text=target)
    fig.show()
    return fig


def plot_metrics_comparison(results: Dict[str, Dict[str, float]]):
    """
    Gráfico comparativo das métricas entre splits (treino, validação, teste).
    """
    splits = list(results.keys())
    metrics_names = list(results[splits[0]].keys())

    fig = make_subplots(
        rows=1, cols=len(metrics_names),
        subplot_titles=metrics_names,
    )

    for i, metric in enumerate(metrics_names):
        values = [results[split][metric] for split in splits]
        fig.add_trace(
            go.Bar(
                x=splits, y=values,
                marker_color=[GPCORP_COLORS["azul_escuro"], GPCORP_COLORS["azul_medio"], GPCORP_COLORS["laranja"]],
                text=[f"{v:.2f}" for v in values],
                textposition="outside",
                name=metric,
                showlegend=False,
            ),
            row=1, col=i + 1,
        )

    _apply_gpcorp_style(fig, title="Comparação de Métricas por Split", height=400)
    fig.show()
    return fig


# ---------------------------------------------------------------------------
# Avaliação completa com visualizações
# ---------------------------------------------------------------------------


def run_full_evaluation(
    model: Pipeline,
    df_test: pd.DataFrame,
    target: str = "quantidade",
    feature_cols: list = None,
) -> Dict:
    """
    Avaliação completa do modelo com métricas e visualizações.

    Parameters
    ----------
    model : Pipeline
        Modelo treinado.
    df_test : pd.DataFrame
        DataFrame de teste completo (com data_venda).
    target : str
        Coluna target.
    feature_cols : list, optional
        Lista de colunas de features.

    Returns
    -------
    Dict
        Resultados com métricas e figuras.
    """
    from sagemaker_training import get_feature_columns

    if feature_cols is None:
        feature_cols = get_feature_columns(df_test, target)

    X_test = df_test[feature_cols]
    y_test = df_test[target]
    y_pred = model.predict(X_test)

    show_log("=" * 60)
    show_log("GP Corp - Avaliação Completa do Modelo")
    show_log("=" * 60)

    # Métricas
    metrics = calculate_metrics(y_test.values, y_pred)
    show_log(f"\nMétricas no Teste:")
    show_log(f"  MAE:  {metrics['MAE']:.4f}")
    show_log(f"  RMSE: {metrics['RMSE']:.4f}")
    show_log(f"  MAPE: {metrics['MAPE']:.2f}%")

    # Visualizações
    show_log("\nGerando visualizações...")

    figs = {}
    figs["pred_vs_real"] = plot_predictions_vs_actual(y_test.values, y_pred)
    figs["residuals"] = plot_residuals(y_test.values, y_pred)
    figs["error_time"] = plot_error_over_time(df_test, y_pred, target=target)
    figs["timeline"] = plot_predictions_timeline(df_test, y_pred, target=target)

    show_log("\nAvaliação concluída.")
    show_log("=" * 60)

    return {
        "metrics": metrics,
        "y_pred": y_pred,
        "figures": figs,
    }
