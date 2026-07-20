"""
SageMaker - Análise Exploratória de Dados (EDA) para Série Temporal GP Corp

Funções para análises interativas com Plotly usando paleta de cores da GP Corp.
Inclui: distribuições, boxplots, nulos, sazonalidade, correlação e mais.

Uso no notebook:
    from sagemaker_eda import *
    df = pd.read_parquet("dataset_serie_temporal.parquet")
    plot_null_analysis(df)
    plot_distribution(df, "quantidade")
    ...
"""

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots

from sagemaker_logs import show_log

# ---------------------------------------------------------------------------
# Paleta de cores GP Corp
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

# Sequência de cores para gráficos multi-série
GPCORP_SEQUENCE = [
    GPCORP_COLORS["azul_escuro"],
    GPCORP_COLORS["laranja"],
    GPCORP_COLORS["azul_medio"],
    GPCORP_COLORS["dourado"],
    GPCORP_COLORS["azul_claro"],
    GPCORP_COLORS["laranja_claro"],
    GPCORP_COLORS["cinza_escuro"],
    GPCORP_COLORS["cinza_medio"],
]

# Template base para todos os gráficos
GPCORP_TEMPLATE = {
    "layout": {
        "font": {"family": "Segoe UI, Arial, sans-serif", "color": GPCORP_COLORS["cinza_escuro"]},
        "paper_bgcolor": GPCORP_COLORS["branco"],
        "plot_bgcolor": "#F8F9FA",
        "title": {"font": {"size": 16, "color": GPCORP_COLORS["azul_escuro"]}},
        "colorway": GPCORP_SEQUENCE,
    }
}


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
# 1. Análise de Nulos
# ---------------------------------------------------------------------------


def plot_null_analysis(df: pd.DataFrame):
    """
    Visualização completa de valores nulos no dataset.
    Gera barras com % de nulos por coluna.
    """
    null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
    null_pct = null_pct[null_pct > 0]

    if null_pct.empty:
        show_log("Nenhum valor nulo encontrado no dataset.")
        return None

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=null_pct.index,
        y=null_pct.values,
        marker_color=GPCORP_COLORS["laranja"],
        text=[f"{v:.1f}%" for v in null_pct.values],
        textposition="outside",
    ))

    _apply_gpcorp_style(fig, title="Percentual de Valores Nulos por Coluna")
    fig.update_yaxes(title_text="% Nulo")
    fig.update_xaxes(title_text="Coluna", tickangle=45)

    show_log(f"Colunas com nulos: {len(null_pct)}")
    fig.show()
    return fig


def null_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Retorna tabela resumo de nulos."""
    null_info = pd.DataFrame({
        "nulos": df.isnull().sum(),
        "pct_nulo": (df.isnull().sum() / len(df) * 100).round(2),
        "dtype": df.dtypes,
    })
    null_info = null_info[null_info["nulos"] > 0].sort_values("nulos", ascending=False)
    return null_info


# ---------------------------------------------------------------------------
# 2. Distribuição dos Dados
# ---------------------------------------------------------------------------


def plot_distribution(df: pd.DataFrame, column: str, nbins: int = 50):
    """
    Histograma interativo da distribuição de uma variável numérica.
    """
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=df[column].dropna(),
        nbinsx=nbins,
        marker_color=GPCORP_COLORS["azul_medio"],
        opacity=0.85,
    ))

    # Linha de média
    mean_val = df[column].mean()
    fig.add_vline(
        x=mean_val, line_dash="dash",
        line_color=GPCORP_COLORS["laranja"],
        annotation_text=f"Média: {mean_val:.2f}",
    )

    _apply_gpcorp_style(fig, title=f"Distribuição: {column}")
    fig.update_xaxes(title_text=column)
    fig.update_yaxes(title_text="Frequência")
    fig.show()
    return fig


def plot_distributions_grid(df: pd.DataFrame, columns: list = None, ncols: int = 3):
    """
    Grid de histogramas para múltiplas variáveis numéricas.
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()

    nrows = -(-len(columns) // ncols)  # ceil division
    fig = make_subplots(rows=nrows, cols=ncols, subplot_titles=columns)

    for i, col in enumerate(columns):
        row = i // ncols + 1
        col_idx = i % ncols + 1
        fig.add_trace(
            go.Histogram(
                x=df[col].dropna(),
                marker_color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)],
                opacity=0.8,
                name=col,
                showlegend=False,
            ),
            row=row, col=col_idx,
        )

    _apply_gpcorp_style(fig, title="Distribuições das Variáveis Numéricas", height=250 * nrows)
    fig.show()
    return fig


# ---------------------------------------------------------------------------
# 3. Boxplots
# ---------------------------------------------------------------------------


def plot_boxplot(df: pd.DataFrame, column: str, group_by: str = None):
    """
    Boxplot interativo para identificar outliers.
    Se group_by for informado, cria boxplots por grupo.
    """
    if group_by:
        fig = px.box(
            df, x=group_by, y=column,
            color=group_by,
            color_discrete_sequence=GPCORP_SEQUENCE,
        )
    else:
        fig = go.Figure()
        fig.add_trace(go.Box(
            y=df[column].dropna(),
            marker_color=GPCORP_COLORS["azul_medio"],
            boxmean="sd",
            name=column,
        ))

    _apply_gpcorp_style(fig, title=f"Boxplot: {column}" + (f" por {group_by}" if group_by else ""))
    fig.show()
    return fig


def plot_boxplots_numeric(df: pd.DataFrame, columns: list = None):
    """
    Boxplots lado a lado para todas as variáveis numéricas (normalizadas para comparação).
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()

    fig = go.Figure()
    for i, col in enumerate(columns):
        fig.add_trace(go.Box(
            y=df[col].dropna(),
            name=col,
            marker_color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)],
        ))

    _apply_gpcorp_style(fig, title="Boxplots das Variáveis Numéricas", height=600)
    fig.update_xaxes(tickangle=45)
    fig.show()
    return fig


# ---------------------------------------------------------------------------
# 3b. Violin Plots
# ---------------------------------------------------------------------------


def plot_violin(df: pd.DataFrame, column: str, group_by: str = None):
    """
    Violin plot interativo para uma variável numérica.
    Mostra distribuição + boxplot embutido.
    Se group_by for informado, cria violinos por grupo.
    """
    if group_by:
        fig = go.Figure()
        groups = df[group_by].value_counts().head(10).index.tolist()
        df_filtered = df[df[group_by].isin(groups)]
        for i, grp in enumerate(groups):
            fig.add_trace(go.Violin(
                y=df_filtered[df_filtered[group_by] == grp][column].dropna(),
                name=str(grp),
                box_visible=True,
                meanline_visible=True,
                line_color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)],
                fillcolor=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)],
                opacity=0.6,
            ))
    else:
        fig = go.Figure()
        fig.add_trace(go.Violin(
            y=df[column].dropna(),
            box_visible=True,
            meanline_visible=True,
            line_color=GPCORP_COLORS["azul_medio"],
            fillcolor=GPCORP_COLORS["azul_claro"],
            opacity=0.7,
            name=column,
        ))

    _apply_gpcorp_style(fig, title=f"Violin Plot: {column}" + (f" por {group_by}" if group_by else ""))
    fig.show()
    return fig


def plot_violins_numeric(df: pd.DataFrame, columns: list = None):
    """
    Violin plots lado a lado para todas as variáveis numéricas.
    Mostra distribuição completa + quartis + média.
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
        # Excluir IDs e temporais derivadas
        exclude = ["vendedor_code", "filial_id", "ano", "mes", "dia",
                   "dia_semana", "trimestre", "semana_ano", "eh_fim_mes"]
        columns = [c for c in columns if c not in exclude]

    fig = go.Figure()
    for i, col in enumerate(columns):
        fig.add_trace(go.Violin(
            y=df[col].dropna(),
            name=col,
            box_visible=True,
            meanline_visible=True,
            line_color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)],
            fillcolor=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)],
            opacity=0.6,
        ))

    _apply_gpcorp_style(fig, title="Violin Plots das Variáveis Numéricas", height=600)
    fig.update_xaxes(tickangle=45)
    fig.show()
    return fig


def plot_violin_targets_por_grupo(df: pd.DataFrame, group_col: str = "item_group"):
    """
    Violin plots dos targets (quantidade e lucro_bruto) agrupados por uma categórica.
    Mostra como a distribuição dos targets varia entre grupos.
    """
    df_temp = df.copy()

    # Top 8 grupos para não poluir
    top_groups = df_temp[group_col].value_counts().head(8).index.tolist()
    df_temp = df_temp[df_temp[group_col].isin(top_groups)]

    fig = make_subplots(rows=2, cols=1, subplot_titles=["Quantidade por Grupo", "Lucro Bruto por Grupo"])

    for i, grp in enumerate(top_groups):
        df_grp = df_temp[df_temp[group_col] == grp]
        fig.add_trace(
            go.Violin(
                y=df_grp["quantidade"].dropna(),
                name=str(grp),
                box_visible=True,
                meanline_visible=True,
                line_color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)],
                fillcolor=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)],
                opacity=0.6,
                legendgroup=str(grp),
                showlegend=True,
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Violin(
                y=df_grp["lucro_bruto"].dropna(),
                name=str(grp),
                box_visible=True,
                meanline_visible=True,
                line_color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)],
                fillcolor=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)],
                opacity=0.6,
                legendgroup=str(grp),
                showlegend=False,
            ),
            row=2, col=1,
        )

    _apply_gpcorp_style(fig, title=f"Distribuição dos Targets por {group_col}", height=800)
    fig.show()
    return fig


# ---------------------------------------------------------------------------
# 4. Análise Temporal / Sazonalidade
# ---------------------------------------------------------------------------

# Colunas temporais derivadas que NÃO devem ser analisadas aqui
_TEMPORAL_DERIVED_COLS = [
    "dia_semana", "trimestre", "semana_ano", "eh_fim_mes",
    "media_movel_7d", "media_movel_30d", "qtd_semana_anterior", "qtd_mes_anterior",
    "ano", "mes", "dia",
]


def plot_vendas_temporal(df: pd.DataFrame, target: str = "quantidade"):
    """
    Série temporal completa com múltiplas agregações em um único gráfico (subplots).
    Mostra a mesma série em: diário, semanal, mensal, trimestral e anual.
    Permite visualizar o comportamento em diferentes granularidades.
    """
    df_temp = df.copy()
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])
    df_temp = df_temp.sort_values("data_venda")

    # Criar séries agregadas
    ts_daily = df_temp.set_index("data_venda")[target].resample("D").sum().reset_index()
    ts_weekly = df_temp.set_index("data_venda")[target].resample("W").sum().reset_index()
    ts_monthly = df_temp.set_index("data_venda")[target].resample("ME").sum().reset_index()
    ts_quarterly = df_temp.set_index("data_venda")[target].resample("QE").sum().reset_index()
    ts_yearly = df_temp.set_index("data_venda")[target].resample("YE").sum().reset_index()

    fig = make_subplots(
        rows=5, cols=1,
        subplot_titles=["Diário", "Semanal", "Mensal", "Trimestral", "Anual"],
        shared_xaxes=False,
        vertical_spacing=0.06,
    )

    series_data = [
        (ts_daily, "Diário", GPCORP_COLORS["cinza_medio"]),
        (ts_weekly, "Semanal", GPCORP_COLORS["azul_claro"]),
        (ts_monthly, "Mensal", GPCORP_COLORS["azul_medio"]),
        (ts_quarterly, "Trimestral", GPCORP_COLORS["azul_escuro"]),
        (ts_yearly, "Anual", GPCORP_COLORS["laranja"]),
    ]

    for i, (ts, name, color) in enumerate(series_data):
        fig.add_trace(
            go.Scatter(
                x=ts["data_venda"], y=ts[target],
                mode="lines+markers" if len(ts) < 30 else "lines",
                line=dict(color=color, width=2),
                marker=dict(size=6) if len(ts) < 30 else None,
                name=name,
            ),
            row=i + 1, col=1,
        )

    _apply_gpcorp_style(fig, title=f"Evolução Temporal: {target} (todas as agregações)", height=1200)
    fig.update_yaxes(title_text=target)
    fig.show()
    return fig


def plot_vendas_temporal_por_ano(df: pd.DataFrame, target: str = "quantidade", freq: str = "W"):
    """
    Série temporal agregada com linhas separadas por ano (sobrepostas).
    Permite comparar o mesmo período entre anos diferentes.
    """
    df_temp = df.copy()
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])
    df_temp["ano"] = df_temp["data_venda"].dt.year
    df_temp = df_temp.sort_values("data_venda")

    fig = go.Figure()
    anos = sorted(df_temp["ano"].unique())

    for i, ano in enumerate(anos):
        df_ano = df_temp[df_temp["ano"] == ano].copy()
        ts = df_ano.set_index("data_venda")[target].resample(freq).sum().reset_index()
        # Usar dia do ano como eixo X para sobreposição
        ts["dia_do_ano"] = ts["data_venda"].dt.dayofyear

        fig.add_trace(go.Scatter(
            x=ts["dia_do_ano"], y=ts[target],
            mode="lines",
            line=dict(color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)], width=2),
            name=str(ano),
        ))

    _apply_gpcorp_style(fig, title=f"Comparação Anual: {target} ({freq})")
    fig.update_xaxes(title_text="Dia do Ano")
    fig.update_yaxes(title_text=target)
    fig.show()
    return fig


def plot_variaveis_numericas_temporal(df: pd.DataFrame, freq: str = "W"):
    """
    Gráfico de linhas com a evolução temporal de TODAS as variáveis numéricas
    (excluindo variáveis temporais derivadas).
    Cada variável em um subplot para facilitar comparação.
    """
    df_temp = df.copy()
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])

    # Selecionar numéricas excluindo temporais derivadas e IDs
    numeric_cols = df_temp.select_dtypes(include=[np.number]).columns.tolist()
    exclude = _TEMPORAL_DERIVED_COLS + ["vendedor_code", "filial_id"]
    cols = [c for c in numeric_cols if c not in exclude]

    nrows = len(cols)
    fig = make_subplots(
        rows=nrows, cols=1,
        subplot_titles=cols,
        shared_xaxes=True,
        vertical_spacing=0.03,
    )

    for i, col in enumerate(cols):
        ts = df_temp.set_index("data_venda")[col].resample(freq).mean().reset_index()
        fig.add_trace(
            go.Scatter(
                x=ts["data_venda"], y=ts[col],
                mode="lines",
                line=dict(color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)], width=2),
                name=col,
            ),
            row=i + 1, col=1,
        )

    _apply_gpcorp_style(
        fig,
        title=f"Evolução Temporal das Variáveis Numéricas (média {freq})",
        height=220 * nrows,
    )
    fig.update_xaxes(title_text="Data", row=nrows, col=1)
    fig.show()
    return fig


def plot_variaveis_categoricas_temporal(df: pd.DataFrame, freq: str = "M"):
    """
    Gráfico de linhas mostrando a contagem/distribuição temporal das variáveis categóricas.
    Para cada categórica, mostra as top 5 categorias ao longo do tempo.
    """
    df_temp = df.copy()
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])

    categorical_cols = ["item_group", "vendedor_nome", "cliente_uf", "armazem"]
    cols = [c for c in categorical_cols if c in df_temp.columns]

    figs = {}
    for col in cols:
        # Top 5 categorias por frequência
        top_cats = df_temp[col].value_counts().head(5).index.tolist()
        df_filtered = df_temp[df_temp[col].isin(top_cats)].copy()

        # Contagem por período e categoria
        ts = (
            df_filtered.groupby([pd.Grouper(key="data_venda", freq=freq), col])
            .size()
            .reset_index(name="contagem")
        )

        fig = go.Figure()
        for j, cat in enumerate(top_cats):
            ts_cat = ts[ts[col] == cat]
            fig.add_trace(go.Scatter(
                x=ts_cat["data_venda"], y=ts_cat["contagem"],
                mode="lines",
                line=dict(color=GPCORP_SEQUENCE[j % len(GPCORP_SEQUENCE)], width=2),
                name=str(cat),
            ))

        _apply_gpcorp_style(fig, title=f"Variação Temporal: {col} (top 5, freq={freq})")
        fig.update_xaxes(title_text="Data")
        fig.update_yaxes(title_text="Contagem de vendas")
        fig.show()
        figs[col] = fig

    return figs


def plot_sazonalidade_mensal(df: pd.DataFrame, target: str = "quantidade"):
    """
    Análise de sazonalidade mensal — linha com média do target por mês.
    """
    df_temp = df.copy()
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])
    df_temp["mes_num"] = df_temp["data_venda"].dt.month

    sazonalidade = (
        df_temp.groupby("mes_num")[target]
        .mean()
        .reset_index()
        .sort_values("mes_num")
    )

    meses = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    sazonalidade["mes_nome"] = sazonalidade["mes_num"].map(lambda x: meses[x - 1])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=sazonalidade["mes_nome"],
        y=sazonalidade[target],
        mode="lines+markers",
        line=dict(color=GPCORP_COLORS["azul_medio"], width=3),
        marker=dict(size=8, color=GPCORP_COLORS["azul_escuro"]),
        name=f"Média {target}",
    ))

    _apply_gpcorp_style(fig, title=f"Sazonalidade Mensal: Média de {target}")
    fig.update_yaxes(title_text=f"Média {target}")
    fig.show()
    return fig


def plot_sazonalidade_dia_semana(df: pd.DataFrame, target: str = "quantidade"):
    """
    Análise de sazonalidade por dia da semana — gráfico de linha.
    """
    df_temp = df.copy()
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])
    df_temp["dia_num"] = df_temp["data_venda"].dt.dayofweek

    sazonalidade = (
        df_temp.groupby("dia_num")[target]
        .mean()
        .reset_index()
        .sort_values("dia_num")
    )

    dias = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    sazonalidade["dia_nome"] = sazonalidade["dia_num"].map(lambda x: dias[x])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=sazonalidade["dia_nome"],
        y=sazonalidade[target],
        mode="lines+markers",
        line=dict(color=GPCORP_COLORS["laranja"], width=3),
        marker=dict(size=8, color=GPCORP_COLORS["laranja"]),
        name=f"Média {target}",
    ))

    _apply_gpcorp_style(fig, title=f"Sazonalidade por Dia da Semana: Média de {target}")
    fig.update_yaxes(title_text=f"Média {target}")
    fig.show()
    return fig


def plot_sazonalidade_trimestral(df: pd.DataFrame, target: str = "quantidade"):
    """
    Análise de sazonalidade trimestral — linhas por ano para comparação.
    """
    df_temp = df.copy()
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])
    df_temp["trimestre"] = df_temp["data_venda"].dt.quarter
    df_temp["ano"] = df_temp["data_venda"].dt.year

    sazonalidade = (
        df_temp.groupby(["ano", "trimestre"])[target]
        .sum()
        .reset_index()
    )

    fig = go.Figure()
    anos = sorted(sazonalidade["ano"].unique())
    for i, ano in enumerate(anos):
        df_ano = sazonalidade[sazonalidade["ano"] == ano]
        fig.add_trace(go.Scatter(
            x=df_ano["trimestre"],
            y=df_ano[target],
            mode="lines+markers",
            line=dict(color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)], width=2),
            marker=dict(size=7),
            name=str(ano),
        ))

    _apply_gpcorp_style(fig, title=f"Sazonalidade Trimestral: {target} por Ano")
    fig.update_xaxes(title_text="Trimestre", dtick=1)
    fig.update_yaxes(title_text=f"Total {target}")
    fig.show()
    return fig


# ---------------------------------------------------------------------------
# 4b. Análise dos Targets (Lucro Bruto e Quantidade)
# ---------------------------------------------------------------------------


def plot_target_analysis(df: pd.DataFrame):
    """
    Análise completa das variáveis target: lucro_bruto e quantidade.
    Inclui evolução temporal, distribuição e relação entre elas.
    """
    df_temp = df.copy()
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])

    figs = {}

    # 1. Evolução temporal de ambos targets (dual axis)
    ts_qty = df_temp.set_index("data_venda")["quantidade"].resample("W").sum().reset_index()
    ts_lucro = df_temp.set_index("data_venda")["lucro_bruto"].resample("W").sum().reset_index()

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=ts_qty["data_venda"], y=ts_qty["quantidade"],
            mode="lines",
            line=dict(color=GPCORP_COLORS["azul_escuro"], width=2),
            name="Quantidade",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=ts_lucro["data_venda"], y=ts_lucro["lucro_bruto"],
            mode="lines",
            line=dict(color=GPCORP_COLORS["laranja"], width=2),
            name="Lucro Bruto",
        ),
        secondary_y=True,
    )
    _apply_gpcorp_style(fig, title="Targets: Quantidade vs Lucro Bruto (semanal)")
    fig.update_yaxes(title_text="Quantidade", secondary_y=False)
    fig.update_yaxes(title_text="Lucro Bruto (R$)", secondary_y=True)
    fig.update_xaxes(title_text="Data")
    fig.show()
    figs["targets_dual_axis"] = fig

    # 2. Scatter: relação entre quantidade e lucro bruto
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=df_temp["quantidade"],
        y=df_temp["lucro_bruto"],
        mode="markers",
        marker=dict(
            color=GPCORP_COLORS["azul_medio"],
            size=4,
            opacity=0.4,
        ),
        name="Vendas",
    ))
    _apply_gpcorp_style(fig2, title="Relação: Quantidade × Lucro Bruto")
    fig2.update_xaxes(title_text="Quantidade Vendida")
    fig2.update_yaxes(title_text="Lucro Bruto (R$)")
    fig2.show()
    figs["scatter_qty_lucro"] = fig2

    # 3. Lucro bruto por grupo de produto (linha temporal)
    if "item_group" in df_temp.columns:
        top_groups = df_temp["item_group"].value_counts().head(5).index.tolist()
        df_top = df_temp[df_temp["item_group"].isin(top_groups)]
        ts_group = (
            df_top.groupby([pd.Grouper(key="data_venda", freq="M"), "item_group"])["lucro_bruto"]
            .sum()
            .reset_index()
        )

        fig3 = go.Figure()
        for i, grp in enumerate(top_groups):
            ts_g = ts_group[ts_group["item_group"] == grp]
            fig3.add_trace(go.Scatter(
                x=ts_g["data_venda"], y=ts_g["lucro_bruto"],
                mode="lines",
                line=dict(color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)], width=2),
                name=str(grp),
            ))
        _apply_gpcorp_style(fig3, title="Lucro Bruto Mensal por Grupo de Produto (top 5)")
        fig3.update_xaxes(title_text="Data")
        fig3.update_yaxes(title_text="Lucro Bruto (R$)")
        fig3.show()
        figs["lucro_por_grupo"] = fig3

    # 4. Quantidade por vendedor (linha temporal)
    if "vendedor_nome" in df_temp.columns:
        top_vendedores = df_temp["vendedor_nome"].value_counts().head(5).index.tolist()
        df_top_v = df_temp[df_temp["vendedor_nome"].isin(top_vendedores)]
        ts_vend = (
            df_top_v.groupby([pd.Grouper(key="data_venda", freq="M"), "vendedor_nome"])["quantidade"]
            .sum()
            .reset_index()
        )

        fig4 = go.Figure()
        for i, vend in enumerate(top_vendedores):
            ts_v = ts_vend[ts_vend["vendedor_nome"] == vend]
            fig4.add_trace(go.Scatter(
                x=ts_v["data_venda"], y=ts_v["quantidade"],
                mode="lines",
                line=dict(color=GPCORP_SEQUENCE[i % len(GPCORP_SEQUENCE)], width=2),
                name=str(vend),
            ))
        _apply_gpcorp_style(fig4, title="Quantidade Mensal por Vendedor (top 5)")
        fig4.update_xaxes(title_text="Data")
        fig4.update_yaxes(title_text="Quantidade")
        fig4.show()
        figs["qtd_por_vendedor"] = fig4

    # 5. Distribuição dos targets
    fig5 = make_subplots(rows=1, cols=2, subplot_titles=["Quantidade", "Lucro Bruto"])
    fig5.add_trace(
        go.Histogram(x=df_temp["quantidade"], marker_color=GPCORP_COLORS["azul_medio"], nbinsx=50, name="Quantidade"),
        row=1, col=1,
    )
    fig5.add_trace(
        go.Histogram(x=df_temp["lucro_bruto"], marker_color=GPCORP_COLORS["laranja"], nbinsx=50, name="Lucro Bruto"),
        row=1, col=2,
    )
    _apply_gpcorp_style(fig5, title="Distribuição dos Targets")
    fig5.show()
    figs["targets_distribuicao"] = fig5

    show_log("Análise de targets (quantidade e lucro_bruto) concluída.")
    return figs


# ---------------------------------------------------------------------------
# 5. Heatmap de Correlação
# ---------------------------------------------------------------------------


def plot_correlation_heatmap(df: pd.DataFrame, columns: list = None):
    """
    Heatmap de correlação interativo para variáveis numéricas.
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()

    corr_matrix = df[columns].corr()

    fig = go.Figure(data=go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns,
        y=corr_matrix.index,
        colorscale=[
            [0.0, GPCORP_COLORS["azul_escuro"]],
            [0.5, GPCORP_COLORS["branco"]],
            [1.0, GPCORP_COLORS["laranja"]],
        ],
        zmin=-1, zmax=1,
        text=np.round(corr_matrix.values, 2),
        texttemplate="%{text}",
        textfont={"size": 9},
    ))

    _apply_gpcorp_style(fig, title="Heatmap de Correlação", height=700)
    fig.update_xaxes(tickangle=45)
    fig.show()
    return fig


# ---------------------------------------------------------------------------
# 6. Análise por Categorias (Top N)
# ---------------------------------------------------------------------------


def plot_top_items(df: pd.DataFrame, n: int = 15, target: str = "quantidade"):
    """
    Top N itens mais vendidos (por quantidade ou valor).
    """
    top = (
        df.groupby(["item_code", "item_name"])[target]
        .sum()
        .reset_index()
        .sort_values(target, ascending=False)
        .head(n)
    )

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=top[target],
        y=top["item_name"],
        orientation="h",
        marker_color=GPCORP_COLORS["azul_medio"],
        text=[f"{v:,.0f}" for v in top[target]],
        textposition="outside",
    ))

    _apply_gpcorp_style(fig, title=f"Top {n} Itens por {target}", height=500)
    fig.update_yaxes(autorange="reversed")
    fig.update_xaxes(title_text=target)
    fig.show()
    return fig


def plot_vendas_por_vendedor(df: pd.DataFrame, n: int = 10, target: str = "valor_total"):
    """
    Vendas por vendedor (top N).
    """
    top = (
        df.groupby("vendedor_nome")[target]
        .sum()
        .reset_index()
        .sort_values(target, ascending=False)
        .head(n)
    )

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=top["vendedor_nome"],
        y=top[target],
        marker_color=GPCORP_COLORS["dourado"],
        text=[f"R$ {v:,.0f}" for v in top[target]],
        textposition="outside",
    ))

    _apply_gpcorp_style(fig, title=f"Top {n} Vendedores por {target}")
    fig.update_xaxes(tickangle=45)
    fig.update_yaxes(title_text=target)
    fig.show()
    return fig


def plot_vendas_por_grupo(df: pd.DataFrame, target: str = "valor_total"):
    """
    Vendas por grupo/categoria de produto.
    """
    grupo = (
        df.groupby("item_group")[target]
        .sum()
        .reset_index()
        .sort_values(target, ascending=False)
    )

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=grupo["item_group"],
        y=grupo[target],
        marker_color=GPCORP_COLORS["azul_escuro"],
        text=[f"R$ {v:,.0f}" for v in grupo[target]],
        textposition="outside",
    ))

    _apply_gpcorp_style(fig, title=f"Vendas por Grupo de Produto ({target})")
    fig.update_xaxes(tickangle=45)
    fig.update_yaxes(title_text=target)
    fig.show()
    return fig


def plot_vendas_por_uf(df: pd.DataFrame, target: str = "valor_total"):
    """
    Distribuição geográfica das vendas por UF.
    """
    uf = (
        df.groupby("cliente_uf")[target]
        .sum()
        .reset_index()
        .sort_values(target, ascending=False)
    )

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=uf["cliente_uf"],
        y=uf[target],
        marker_color=GPCORP_COLORS["azul_claro"],
        text=[f"R$ {v:,.0f}" for v in uf[target]],
        textposition="outside",
    ))

    _apply_gpcorp_style(fig, title=f"Vendas por UF ({target})")
    fig.update_xaxes(title_text="UF")
    fig.update_yaxes(title_text=target)
    fig.show()
    return fig


# ---------------------------------------------------------------------------
# 7. Análise de Tendência e Decomposição
# ---------------------------------------------------------------------------


def plot_tendencia_media_movel(df: pd.DataFrame, target: str = "quantidade", window: int = 30):
    """
    Série temporal com média móvel sobreposta para visualizar tendência.
    """
    df_temp = df.copy()
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])
    ts = df_temp.set_index("data_venda")[target].resample("D").sum().reset_index()
    ts["media_movel"] = ts[target].rolling(window=window, min_periods=1).mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ts["data_venda"], y=ts[target],
        mode="lines",
        line=dict(color=GPCORP_COLORS["cinza_claro"], width=1),
        name=f"{target} (diário)",
        opacity=0.6,
    ))
    fig.add_trace(go.Scatter(
        x=ts["data_venda"], y=ts["media_movel"],
        mode="lines",
        line=dict(color=GPCORP_COLORS["azul_escuro"], width=3),
        name=f"Média Móvel ({window}d)",
    ))

    _apply_gpcorp_style(fig, title=f"Tendência: {target} com Média Móvel {window} dias")
    fig.update_xaxes(title_text="Data")
    fig.update_yaxes(title_text=target)
    fig.show()
    return fig


# ---------------------------------------------------------------------------
# 8. Resumo Estatístico Visual
# ---------------------------------------------------------------------------


def plot_summary_stats(df: pd.DataFrame):
    """
    Painel resumo com métricas-chave do dataset.
    """
    df_temp = df.copy()
    df_temp["data_venda"] = pd.to_datetime(df_temp["data_venda"])

    stats = {
        "Total Registros": f"{len(df):,}",
        "Período": f"{df_temp['data_venda'].min().strftime('%d/%m/%Y')} a {df_temp['data_venda'].max().strftime('%d/%m/%Y')}",
        "Itens Únicos": f"{df['item_code'].nunique():,}",
        "Vendedores": f"{df['vendedor_code'].nunique():,}",
        "UFs": f"{df['cliente_uf'].nunique():,}",
        "Qtd Total Vendida": f"{df['quantidade'].sum():,.0f}",
        "Valor Total": f"R$ {df['valor_total'].sum():,.2f}",
        "Ticket Médio": f"R$ {df['valor_total'].mean():,.2f}",
    }

    fig = go.Figure()
    fig.add_trace(go.Table(
        header=dict(
            values=["Métrica", "Valor"],
            fill_color=GPCORP_COLORS["azul_escuro"],
            font=dict(color="white", size=13),
            align="left",
        ),
        cells=dict(
            values=[list(stats.keys()), list(stats.values())],
            fill_color=[["#F0F4F8"] * len(stats), [GPCORP_COLORS["branco"]] * len(stats)],
            font=dict(size=12),
            align="left",
            height=30,
        ),
    ))

    _apply_gpcorp_style(fig, title="Resumo do Dataset", height=350)
    fig.show()
    return fig


# ---------------------------------------------------------------------------
# 9. Análise Completa (executa todas)
# ---------------------------------------------------------------------------


def run_full_eda(df: pd.DataFrame):
    """
    Executa todas as análises EDA de uma vez.
    Retorna dicionário com todas as figuras.
    """
    show_log("=" * 60)
    show_log("GP Corp - Análise Exploratória de Dados (EDA)")
    show_log("=" * 60)

    figs = {}

    show_log("\n1. Resumo estatístico...")
    figs["summary"] = plot_summary_stats(df)

    show_log("\n2. Análise de nulos...")
    figs["nulls"] = plot_null_analysis(df)

    show_log("\n3. Distribuições numéricas...")
    numeric_cols = ["quantidade", "valor_total", "preco_unitario", "desconto_pct", "lucro_bruto"]
    existing_cols = [c for c in numeric_cols if c in df.columns]
    figs["distributions"] = plot_distributions_grid(df, columns=existing_cols)

    show_log("\n4. Boxplots...")
    figs["boxplots"] = plot_boxplots_numeric(df, columns=existing_cols)

    show_log("\n4b. Violin plots...")
    figs["violins"] = plot_violins_numeric(df, columns=existing_cols)
    figs["violin_targets_grupo"] = plot_violin_targets_por_grupo(df)

    show_log("\n5. Série temporal...")
    figs["temporal_completo"] = plot_vendas_temporal(df)
    figs["temporal_por_ano"] = plot_vendas_temporal_por_ano(df)
    figs["tendencia"] = plot_tendencia_media_movel(df)

    show_log("\n5b. Variação temporal das variáveis numéricas...")
    figs["numericas_temporal"] = plot_variaveis_numericas_temporal(df)

    show_log("\n5c. Variação temporal das variáveis categóricas...")
    figs["categoricas_temporal"] = plot_variaveis_categoricas_temporal(df)

    show_log("\n6. Sazonalidade...")
    figs["sazonalidade_mensal"] = plot_sazonalidade_mensal(df)
    figs["sazonalidade_dia_semana"] = plot_sazonalidade_dia_semana(df)
    figs["sazonalidade_trimestral"] = plot_sazonalidade_trimestral(df)

    show_log("\n6b. Análise dos targets (quantidade e lucro_bruto)...")
    figs["targets"] = plot_target_analysis(df)

    show_log("\n7. Correlação...")
    figs["correlacao"] = plot_correlation_heatmap(df, columns=existing_cols)

    show_log("\n8. Top itens e vendedores...")
    figs["top_items"] = plot_top_items(df)
    figs["top_vendedores"] = plot_vendas_por_vendedor(df)
    figs["vendas_grupo"] = plot_vendas_por_grupo(df)
    figs["vendas_uf"] = plot_vendas_por_uf(df)

    show_log("\nEDA completa.")
    show_log("=" * 60)

    return figs
