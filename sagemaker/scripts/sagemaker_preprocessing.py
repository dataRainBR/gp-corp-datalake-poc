"""
SageMaker - Preprocessing Pipeline para Modelo de Série Temporal (Previsão de Vendas)

Lê o dataset em Parquet gerado pelo sagemaker_dataset_builder.py e aplica:
1. Verificação e tratamento de nulos
2. Criação de variáveis temporais derivadas
3. Separação em treino, validação e teste (temporal split)
4. Pipeline organizado com scikit-learn

Referência: modelo_serie_temporal_colunas.md (seção 3 - Features Derivadas)
"""

import os
from datetime import datetime
from typing import Tuple

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline

from sagemaker_logs import show_log

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
INPUT_PATH = os.environ.get("INPUT_PATH", "/mnt/custom-file-systems/s3/shared")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/mnt/custom-file-systems/s3/shared")

# ---------------------------------------------------------------------------
# Variáveis
# ---------------------------------------------------------------------------
REMOVE_COLUMNS = [

    # Vazamento
    "log_quantidade",
    'log_valor_total',
    
    "ticket_medio",
    "numero_pedidos",
    "clientes_unicos",

    "receita_por_cliente",
    "quantidade_por_cliente",
    "receita_unitaria",

    "houve_venda",
    "lucro_unitario",
    "lucro_bruto",
    "margem_bruta",
    "margem_bruta_pct",

    # Tendência
    "trend_quantidade",
    "trend_valor",
    "growth_quantidade",

    # pct_change instáveis
    "quantidade_pct_change_1",
    "quantidade_pct_change_7",

    "valor_total_pct_change_1",
    "valor_total_pct_change_7",

    "ticket_medio_pct_change_1",
    "ticket_medio_pct_change_7",

    "lucro_bruto_pct_change_1",
    "lucro_bruto_pct_change_7",

    "margem_bruta_pct_pct_change_1",
    "margem_bruta_pct_pct_change_7",

    # diferenças
    "valor_total_diff_1",
    "valor_total_diff_7",

    "lucro_bruto_diff_1",
    "lucro_bruto_diff_7",
]

def remove_columns(df: pd.DataFrame, remove_columns=REMOVE_COLUMNS)-> pd.DataFrame:
    df=df.drop(remove_columns, axis=1)
    return df

# ---------------------------------------------------------------------------
# Transformers customizados para o Pipeline
# ---------------------------------------------------------------------------
class NullHandler(BaseEstimator, TransformerMixin):
    """
    Verificação e tratamento de valores nulos.

    Estratégia:
    - Colunas numéricas: preenche com 0 (desconto, lucro) ou mediana (preço)
    - Colunas categóricas: preenche com 'DESCONHECIDO'
    - Loga o estado de nulos antes e depois
    """

    def __init__(self):
        self.numeric_fill = {}
        self.categorical_cols = [
            "item_name", "item_group", "vendedor_nome",
            "cliente_nome", "cliente_cidade", "cliente_uf", "armazem"
        ]
        self.zero_fill_cols = [
            "desconto_pct", "lucro_bruto"
        ]
        self.median_fill_cols = [
            "preco_unitario", "valor_total", "quantidade"
        ]

    def fit(self, X: pd.DataFrame, y=None):
        # Calcular medianas das colunas numéricas para preenchimento
        for col in self.median_fill_cols:
            if col in X.columns:
                self.numeric_fill[col] = X[col].median()
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        X = X.copy()

        # Log estado de nulos
        null_counts = X.isnull().sum()
        total_nulls = null_counts[null_counts > 0]
        if len(total_nulls) > 0:
            show_log(f"Nulos encontrados:\n{total_nulls.to_string()}")
        else:
            show_log("Nenhum valor nulo encontrado no dataset.")

        # Preencher categóricas com 'DESCONHECIDO'
        for col in self.categorical_cols:
            if col in X.columns:
                X[col] = X[col].fillna("DESCONHECIDO")

        # Preencher numéricas com 0
        for col in self.zero_fill_cols:
            if col in X.columns:
                X[col] = X[col].fillna(0)

        # Preencher numéricas com mediana
        for col in self.median_fill_cols:
            if col in X.columns and col in self.numeric_fill:
                X[col] = X[col].fillna(self.numeric_fill[col])

        # Preencher vendedor_code e filial_id com -1 (indicador de ausência)
        for col in ["vendedor_code", "filial_id"]:
            if col in X.columns:
                X[col] = X[col].fillna(-1)

        # Log pós-tratamento
        remaining_nulls = X.isnull().sum().sum()
        show_log(f"Nulos após tratamento: {remaining_nulls}")

       
        return X

class SalesAggregator(BaseEstimator, TransformerMixin):
    """
    Agrega o dataset transacional em uma série temporal diária.

    Granularidade final:

        data_venda
        item_code
        filial_id
        vendedor_code

    Cada linha representa a venda diária de um produto realizada
    por um vendedor em uma filial.

    Features criadas:

        quantidade
        valor_total
        lucro_bruto
        preco_unitario
        desconto_pct
        clientes_unicos
        numero_pedidos
        ticket_medio
        margem_bruta
    """

    def __init__(self):

        self.group_columns = [
            "data_venda",
            "item_code",
            "filial_id",
            "vendedor_code",
        ]

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X = X.copy()

        X["data_venda"] = pd.to_datetime(X["data_venda"])

        ###################################################################
        # preço ponderado
        ###################################################################

        X["valor_sem_desconto"] = (
            X["preco_unitario"] *
            X["quantidade"]
        )

        ###################################################################
        # agregação
        ###################################################################

        df = (

            X.groupby(
                self.group_columns,
                as_index=False
            )

            .agg(

                quantidade=(
                    "quantidade",
                    "sum"
                ),

                valor_total=(
                    "valor_total",
                    "sum"
                ),

                lucro_bruto=(
                    "lucro_bruto",
                    "sum"
                ),

                valor_sem_desconto=(
                    "valor_sem_desconto",
                    "sum"
                ),

                desconto_pct=(
                    "desconto_pct",
                    "mean"
                ),

                item_name=(
                    "item_name",
                    "first"
                ),

                item_group=(
                    "item_group",
                    "first"
                ),

                vendedor_nome=(
                    "vendedor_nome",
                    "first"
                ),

                armazem=(
                    "armazem",
                    "first"
                ),

                cliente_uf=(
                    "cliente_uf",
                    "first"
                ),

                cliente_cidade=(
                    "cliente_cidade",
                    "first"
                ),

                clientes_unicos=(
                    "cliente_code",
                    pd.Series.nunique
                ),

                numero_pedidos=(
                    "cliente_code",
                    "count"
                )

            )

        )

        ###################################################################
        # preço médio ponderado
        ###################################################################

        df["preco_unitario"] = (
            df["valor_sem_desconto"] /
            df["quantidade"]
        )

        ###################################################################
        # ticket médio
        ###################################################################

        df["ticket_medio"] = (
            df["valor_total"] /
            df["numero_pedidos"]
        )

        ###################################################################
        # margem
        ###################################################################

        df["margem_bruta"] = np.where(

            df["valor_total"] > 0,

            df["lucro_bruto"] /
            df["valor_total"],

            0

        )

        ###################################################################
        # remover coluna auxiliar
        ###################################################################

        df.drop(
            columns="valor_sem_desconto",
            inplace=True
        )

        ###################################################################
        # ordenar série temporal
        ###################################################################

        df = df.sort_values(
            self.group_columns
        ).reset_index(drop=True)
        
        show_log(f"SalesAggregator aplicado")

        return df


class CalendarCompleter(BaseEstimator, TransformerMixin):
    """
    Completa o calendário diário de cada série temporal.

    Cada série é definida por:

        item_code
        filial_id
        vendedor_code

    Para todos os dias entre a primeira e a última venda
    será criada uma linha.

    Dias sem venda recebem quantidade=0.

    Isso permite que os lags e médias móveis sejam
    calculados corretamente em dias corridos.
    """

    def __init__(self):

        self.series_columns = [
            "item_code",
            "filial_id",
            "vendedor_code"
        ]

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X = X.copy()

        X["data_venda"] = pd.to_datetime(X["data_venda"])

        resultado = []

        ##################################################################
        # uma série temporal por item/filial/vendedor
        ##################################################################

        grupos = X.groupby(self.series_columns)

        for chave, grupo in grupos:

            grupo = grupo.sort_values("data_venda")

            inicio = grupo["data_venda"].min()

            fim = grupo["data_venda"].max()

            calendario = pd.DataFrame({

                "data_venda": pd.date_range(
                    inicio,
                    fim,
                    freq="D"
                )

            })

            ##################################################################
            # adiciona chaves da série
            ##################################################################

            for coluna, valor in zip(self.series_columns, chave):

                calendario[coluna] = valor

            ##################################################################
            # merge
            ##################################################################

            serie = calendario.merge(

                grupo,

                how="left",

                on=[
                    "data_venda",
                    "item_code",
                    "filial_id",
                    "vendedor_code"
                ]

            )

            ##################################################################
            # preencher métricas numéricas
            ##################################################################

            serie["quantidade"] = serie["quantidade"].fillna(0)

            serie["valor_total"] = serie["valor_total"].fillna(0)

            serie["lucro_bruto"] = serie["lucro_bruto"].fillna(0)

            serie["numero_pedidos"] = serie["numero_pedidos"].fillna(0)

            serie["clientes_unicos"] = serie["clientes_unicos"].fillna(0)

            ##################################################################
            # manter cadastro do produto
            ##################################################################

            for coluna in [

                "item_name",

                "item_group",

                "vendedor_nome",

                "armazem",

                "cliente_cidade",

                "cliente_uf"

            ]:

                if coluna in serie.columns:

                    serie[coluna] = (

                        serie[coluna]

                        .ffill()

                        .bfill()

                    )

            ##################################################################
            # preço
            ##################################################################

            if "preco_unitario" in serie.columns:

                serie["preco_unitario"] = (

                    serie["preco_unitario"]

                    .ffill()

                    .bfill()

                )

            ##################################################################
            # desconto
            ##################################################################

            if "desconto_pct" in serie.columns:

                serie["desconto_pct"] = (

                    serie["desconto_pct"]

                    .fillna(0)

                )

            ##################################################################
            # ticket
            ##################################################################

            if "ticket_medio" in serie.columns:

                serie["ticket_medio"] = (

                    serie["ticket_medio"]

                    .fillna(0)

                )

            ##################################################################
            # margem
            ##################################################################

            if "margem_bruta" in serie.columns:

                serie["margem_bruta"] = (

                    serie["margem_bruta"]

                    .fillna(0)

                )

            resultado.append(serie)

        resultado = pd.concat(resultado)

        resultado = resultado.sort_values(

            [

                "item_code",

                "filial_id",

                "vendedor_code",

                "data_venda"

            ]

        ).reset_index(drop=True)
        show_log(f"CalendarCompleter aplicado")

          
        return resultado


class TemporalFeatureCreator(BaseEstimator, TransformerMixin):
    """
    Cria apenas features de calendário.

    Não cria lags.
    Não cria rolling.
    Não utiliza nenhuma variável alvo.

    Essas features podem ser calculadas tanto no treino
    quanto na inferência.
    """

    def __init__(self):

        self.add_cyclic = True

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X = X.copy()

        X["data_venda"] = pd.to_datetime(X["data_venda"])

        ###################################################################
        # Componentes básicos
        ###################################################################

        X["ano"] = X["data_venda"].dt.year

        X["mes"] = X["data_venda"].dt.month

        X["dia"] = X["data_venda"].dt.day

        X["dia_semana"] = X["data_venda"].dt.dayofweek

        X["dia_ano"] = X["data_venda"].dt.dayofyear

        X["semana_ano"] = (
            X["data_venda"]
            .dt
            .isocalendar()
            .week
            .astype(int)
        )

        X["trimestre"] = X["data_venda"].dt.quarter

        ###################################################################
        # Flags
        ###################################################################

        X["eh_fim_semana"] = (

            X["dia_semana"] >= 5

        ).astype(int)

        X["eh_inicio_mes"] = (

            X["dia"] <= 5

        ).astype(int)

        X["eh_fim_mes"] = (

            X["data_venda"].dt.is_month_end

        ).astype(int)

        X["eh_inicio_trimestre"] = (

            X["data_venda"].dt.is_quarter_start

        ).astype(int)

        X["eh_fim_trimestre"] = (

            X["data_venda"].dt.is_quarter_end

        ).astype(int)

        ###################################################################
        # Features cíclicas
        ###################################################################

        if self.add_cyclic:

            X["mes_sin"] = np.sin(

                2 * np.pi *

                X["mes"] / 12

            )

            X["mes_cos"] = np.cos(

                2 * np.pi *

                X["mes"] / 12

            )

            X["dia_semana_sin"] = np.sin(

                2 * np.pi *

                X["dia_semana"] / 7

            )

            X["dia_semana_cos"] = np.cos(

                2 * np.pi *

                X["dia_semana"] / 7

            )

            X["dia_ano_sin"] = np.sin(

                2 * np.pi *

                X["dia_ano"] / 365.25

            )

            X["dia_ano_cos"] = np.cos(

                2 * np.pi *

                X["dia_ano"] / 365.25

            )

        ###################################################################
        # Ordenação
        ###################################################################

        X = X.sort_values(

            [

                "item_code",

                "filial_id",

                "vendedor_code",

                "data_venda"

            ]

        ).reset_index(drop=True)
        show_log(f"TemporalFeatureCreator aplicado")

        
        return X

class LagFeatureCreator(BaseEstimator, TransformerMixin):
    """
    Cria variáveis de atraso (lag) para séries temporais.

    Os lags são calculados separadamente para cada série:

        item_code
        filial_id
        vendedor_code

    Todos os lags utilizam somente informações passadas.
    """

    def __init__(
        self,
        target_columns=None,
        lags=None,
    ):

        if target_columns is None:
            target_columns = [
                "quantidade",
                "valor_total"
            ]

        if lags is None:
            lags = [
                1,
                2,
                3,
                7,
                14,
                21,
                30,
                60,
                90
            ]

        self.target_columns = target_columns
        self.lags = lags

        self.group_columns = [

            "item_code",

            "filial_id",

            "vendedor_code"

        ]

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X = X.copy()

        X = X.sort_values(

            self.group_columns +

            ["data_venda"]

        )

        grouped = X.groupby(self.group_columns)

        ###############################################################

        for target in self.target_columns:

            for lag in self.lags:

                feature_name = f"{target}_lag_{lag}"

                X[feature_name] = (

                    grouped[target]

                    .shift(lag)

                )

        ###############################################################
        # preencher início da série
        ###############################################################

        lag_columns = [

            c

            for c in X.columns

            if "_lag_" in c

        ]

        X[lag_columns] = X[lag_columns].fillna(0)
        show_log(f"LagFeatureCreator aplicado")
        
        return X


class RollingFeatureCreator(BaseEstimator, TransformerMixin):
    """
    Cria estatísticas móveis para séries temporais.

    Todas as estatísticas utilizam apenas informações passadas
    (shift(1)), evitando vazamento de informação.

    Features geradas:

        rolling_mean
        rolling_std
        rolling_min
        rolling_max
        rolling_sum
        rolling_nonzero
        rolling_q95
        ewm_mean
    """

    def __init__(
        self,
        target_columns=None,
        group_columns=None,
        windows=None,
    ):

        self.target_columns = (
            target_columns
            if target_columns is not None
            else [
                "quantidade",
                "valor_total",
            ]
        )

        self.group_columns = (
            group_columns
            if group_columns is not None
            else [
                "item_code",
                "filial_id",
                "vendedor_code",
            ]
        )

        self.windows = (
            windows
            if windows is not None
            else [7, 14, 30]
        )

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X = X.copy()

        X = (
            X.sort_values(
                self.group_columns + ["data_venda"]
            )
            .reset_index(drop=True)
        )

        for target in self.target_columns:

            if target not in X.columns:
                continue

            grouped = X.groupby(self.group_columns)[target]

            ###########################################################
            # Rolling Statistics
            ###########################################################

            for window in self.windows:

                # Média móvel
                X[f"{target}_rolling_mean_{window}"] = grouped.transform(
                    lambda s:
                        s.shift(1)
                         .rolling(window, min_periods=1)
                         .mean()
                )

                # Soma móvel
                X[f"{target}_rolling_sum_{window}"] = grouped.transform(
                    lambda s:
                        s.shift(1)
                         .rolling(window, min_periods=1)
                         .sum()
                )

                # Número de dias com venda (>0)
                X[f"{target}_rolling_nonzero_{window}"] = grouped.transform(
                    lambda s:
                        s.shift(1)
                         .gt(0)
                         .rolling(window, min_periods=1)
                         .sum()
                )

                # Desvio padrão
                X[f"{target}_rolling_std_{window}"] = grouped.transform(
                    lambda s:
                        s.shift(1)
                         .rolling(window, min_periods=2)
                         .std()
                )

                # Mínimo
                X[f"{target}_rolling_min_{window}"] = grouped.transform(
                    lambda s:
                        s.shift(1)
                         .rolling(window, min_periods=1)
                         .min()
                )

                # Máximo
                X[f"{target}_rolling_max_{window}"] = grouped.transform(
                    lambda s:
                        s.shift(1)
                         .rolling(window, min_periods=1)
                         .max()
                )

                # Percentil 95
                X[f"{target}_rolling_q95_{window}"] = grouped.transform(
                    lambda s:
                        s.shift(1)
                         .rolling(window, min_periods=1)
                         .quantile(0.95)
                )

            ###########################################################
            # Exponential Weighted Mean
            ###########################################################

            for span in [7, 14, 30]:

                X[f"{target}_ewm_mean_{span}"] = grouped.transform(
                    lambda s:
                        s.shift(1)
                         .ewm(
                             span=span,
                             adjust=False
                         )
                         .mean()
                )

        ###############################################################
        # Preencher NaN
        ###############################################################

        rolling_cols = [

            c

            for c in X.columns

            if (
                "_rolling_" in c
                or "_ewm_" in c
            )

        ]

        X[rolling_cols] = X[rolling_cols].fillna(0)

        show_log("RollingFeatureCreator aplicado")

        return X

class DemandFeatureCreator(BaseEstimator, TransformerMixin):
    """
    Cria features relacionadas ao comportamento da demanda.

    Todas as features utilizam apenas informações passadas,
    evitando vazamento de informação.

    Features geradas
    ----------------

    quantidade_days_since_last_sale
    valor_total_days_since_last_sale

    quantidade_days_since_max_30
    valor_total_days_since_max_30

    """

    def __init__(
        self,
        target_columns=None,
        group_columns=None,
        rolling_window=30,
    ):

        self.target_columns = (
            target_columns
            if target_columns is not None
            else [
                "quantidade",
                "valor_total",
            ]
        )

        self.group_columns = (
            group_columns
            if group_columns is not None
            else [
                "item_code",
                "filial_id",
                "vendedor_code",
            ]
        )

        self.rolling_window = rolling_window

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X = X.copy()

        X = (
            X.sort_values(
                self.group_columns + ["data_venda"]
            )
            .reset_index(drop=True)
        )

        ###############################################################
        # Processar cada grupo separadamente
        ###############################################################

        for _, idx in X.groupby(self.group_columns).groups.items():

            g = X.loc[idx].copy()

            ###########################################################
            # Para cada variável alvo
            ###########################################################

            for target in self.target_columns:

                if target not in g.columns:
                    continue

                values = g[target].values

                #######################################################
                # Dias desde última venda
                #######################################################

                days_since = np.zeros(len(g), dtype=np.int32)

                last_sale = -1

                for i in range(len(g)):

                    if values[i] > 0:

                        days_since[i] = 0
                        last_sale = i

                    else:

                        if last_sale == -1:

                            days_since[i] = 999

                        else:

                            days_since[i] = i - last_sale

                X.loc[idx, f"{target}_days_since_last_sale"] = days_since

                #######################################################
                # Dias desde o maior valor da janela
                #######################################################

                days_since_max = np.zeros(len(g), dtype=np.int32)

                for i in range(len(g)):

                    start = max(0, i - self.rolling_window)

                    hist = values[start:i]

                    if len(hist) == 0:

                        days_since_max[i] = self.rolling_window

                    else:

                        pos = np.argmax(hist)

                        days_since_max[i] = len(hist) - pos

                X.loc[idx, f"{target}_days_since_max_{self.rolling_window}"] = (
                    days_since_max
                )

        ###############################################################
        # Ajuste de tipos
        ###############################################################

        demand_cols = [

            c

            for c in X.columns

            if (
                "days_since" in c
            )

        ]

        X[demand_cols] = (

            X[demand_cols]

            .fillna(999)

            .astype(np.int16)

        )

        show_log("DemandFeatureCreator aplicado")

        return X
    
class BusinessFeatureCreator(BaseEstimator, TransformerMixin):
    """
    Cria indicadores comerciais derivados.

    Não utiliza informações futuras.

    Todas as features são calculadas apenas utilizando
    as informações disponíveis na própria linha.
    """

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X = X.copy()

        ###############################################################
        # Ticket médio
        ###############################################################

        if (
            "valor_total" in X.columns and
            "numero_pedidos" in X.columns
        ):

            X["ticket_medio"] = np.where(

                X["numero_pedidos"] > 0,

                X["valor_total"] /
                X["numero_pedidos"],

                0

            )

        ###############################################################
        # Lucro unitário
        ###############################################################

        if (
            "lucro_bruto" in X.columns and
            "quantidade" in X.columns
        ):

            X["lucro_unitario"] = np.where(

                X["quantidade"] > 0,

                X["lucro_bruto"] /
                X["quantidade"],

                0

            )

        ###############################################################
        # Margem percentual
        ###############################################################

        if (
            "valor_total" in X.columns and
            "lucro_bruto" in X.columns
        ):

            X["margem_bruta_pct"] = np.where(

                X["valor_total"] > 0,

                X["lucro_bruto"] /
                X["valor_total"],

                0

            )

        ###############################################################
        # Receita por cliente
        ###############################################################

        if (
            "clientes_unicos" in X.columns and
            "valor_total" in X.columns
        ):

            X["receita_por_cliente"] = np.where(

                X["clientes_unicos"] > 0,

                X["valor_total"] /
                X["clientes_unicos"],

                0

            )

        ###############################################################
        # Quantidade por cliente
        ###############################################################

        if (
            "clientes_unicos" in X.columns and
            "quantidade" in X.columns
        ):

            X["quantidade_por_cliente"] = np.where(

                X["clientes_unicos"] > 0,

                X["quantidade"] /
                X["clientes_unicos"],

                0

            )

        ###############################################################
        # Receita por pneu
        ###############################################################

        if (
            "valor_total" in X.columns and
            "quantidade" in X.columns
        ):

            X["receita_unitaria"] = np.where(

                X["quantidade"] > 0,

                X["valor_total"] /
                X["quantidade"],

                0

            )

        ###############################################################
        # Intensidade do desconto
        ###############################################################

        if "desconto_pct" in X.columns:

            X["desconto_intenso"] = (

                X["desconto_pct"] >= 10

            ).astype(int)

        ###############################################################
        # Venda ocorreu
        ###############################################################

        if "quantidade" in X.columns:

            X["houve_venda"] = (

                X["quantidade"] > 0

            ).astype(int)

        ###############################################################
        # Log da quantidade
        ###############################################################

        if "quantidade" in X.columns:

            X["log_quantidade"] = np.log1p(

                X["quantidade"]

            )

        ###############################################################
        # Log do faturamento
        ###############################################################

        if "valor_total" in X.columns:

            X["log_valor_total"] = np.log1p(

                X["valor_total"]

            )
        show_log(f"BusinessFeatureCreator aplicado")

        return X

class PriceFeatureCreator(BaseEstimator, TransformerMixin):
    """
    Cria features relacionadas a preço e desconto utilizando
    apenas informações históricas.

    Todas as features são calculadas para cada série temporal:

        item_code
        filial_id
        vendedor_code

    evitando vazamento de dados.
    """

    def __init__(self):

        self.group_columns = [
            "item_code",
            "filial_id",
            "vendedor_code"
        ]

        self.windows = [7, 14, 30]

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X = X.copy()

        X = X.sort_values(
            self.group_columns + ["data_venda"]
        )

        grouped = X.groupby(self.group_columns)

        ###############################################################
        # PREÇO
        ###############################################################

        if "preco_unitario" in X.columns:

            # lags
            X["preco_lag_1"] = grouped["preco_unitario"].shift(1)
            X["preco_lag_7"] = grouped["preco_unitario"].shift(7)
            X["preco_lag_30"] = grouped["preco_unitario"].shift(30)

            # rolling
            for window in self.windows:

                X[f"preco_rolling_mean_{window}"] = (
                    grouped["preco_unitario"]
                    .transform(
                        lambda s:
                        s.shift(1)
                         .rolling(window, min_periods=1)
                         .mean()
                    )
                )

                X[f"preco_rolling_std_{window}"] = (
                    grouped["preco_unitario"]
                    .transform(
                        lambda s:
                        s.shift(1)
                         .rolling(window, min_periods=2)
                         .std()
                    )
                )

            # diferença absoluta
            X["preco_diff_1"] = (
                grouped["preco_unitario"]
                .transform(lambda s: s.diff(1).shift(1))
            )

            X["preco_diff_7"] = (
                grouped["preco_unitario"]
                .transform(lambda s: s.diff(7).shift(1))
            )

            # variação percentual
            X["preco_pct_change_1"] = (
                grouped["preco_unitario"]
                .transform(lambda s: s.pct_change(1).shift(1))
            )

            X["preco_pct_change_7"] = (
                grouped["preco_unitario"]
                .transform(lambda s: s.pct_change(7).shift(1))
            )

        ###############################################################
        # DESCONTO
        ###############################################################

        if "desconto_pct" in X.columns:

            X["desconto_lag_1"] = grouped["desconto_pct"].shift(1)

            X["desconto_lag_7"] = grouped["desconto_pct"].shift(7)

            for window in self.windows:

                X[f"desconto_rolling_mean_{window}"] = (

                    grouped["desconto_pct"]

                    .transform(

                        lambda s:

                        s.shift(1)

                         .rolling(window, min_periods=1)

                         .mean()

                    )

                )

            X["desconto_pct_change_1"] = (

                grouped["desconto_pct"]

                .transform(

                    lambda s:

                    s.pct_change(1).shift(1)

                )

            )

        ###############################################################
        # ELASTICIDADE SIMPLIFICADA
        ###############################################################

        if (
            "preco_rolling_mean_7" in X.columns and
            "preco_rolling_mean_30" in X.columns
        ):

            X["indice_preco"] = np.where(

                X["preco_rolling_mean_30"] > 0,

                X["preco_rolling_mean_7"] /
                X["preco_rolling_mean_30"],

                1

            )

        ###############################################################
        # LIMPEZA
        ###############################################################

        X.replace(
            [np.inf, -np.inf],
            0,
            inplace=True
        )

        X.fillna(0, inplace=True)
        show_log(f"PriceFeatureCreator aplicado")

        return X

class TrendFeatureCreator(BaseEstimator, TransformerMixin):
    """
    Cria features de tendência utilizando somente dados passados.

    Todas as features são calculadas por

        item_code
        filial_id
        vendedor_code

    evitando vazamento de informação.
    """

    def __init__(self):

        self.group_columns = [

            "item_code",

            "filial_id",

            "vendedor_code"

        ]

        self.columns = [

            "quantidade",

            "valor_total",

            "preco_unitario",

            "ticket_medio",

            "lucro_bruto",

            "margem_bruta_pct"

        ]

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        X = X.copy()

        X = X.sort_values(

            self.group_columns +

            ["data_venda"]

        )

        grouped = X.groupby(self.group_columns)

        ###############################################################

        for col in self.columns:

            if col not in X.columns:
                continue

            ###########################################################
            # variação diária
            ###########################################################

            X[f"{col}_pct_change_1"] = (

                grouped[col]

                .transform(

                    lambda s:

                    s.shift(1).pct_change()

                )

            )

            ###########################################################
            # variação semanal
            ###########################################################

            X[f"{col}_pct_change_7"] = (

                grouped[col]

                .transform(

                    lambda s:

                    s.shift(7).pct_change()

                )

            )

            ###########################################################
            # diferença para o dia anterior
            ###########################################################

            X[f"{col}_diff_1"] = (

                grouped[col]

                .transform(

                    lambda s:

                    s.diff()

                )

            )

            ###########################################################
            # diferença para sete dias
            ###########################################################

            X[f"{col}_diff_7"] = (

                grouped[col]

                .transform(

                    lambda s:

                    s.diff(7)

                )

            )

        ###############################################################
        # tendência curta
        ###############################################################

        if (

            "quantidade_rolling_mean_7" in X.columns and

            "quantidade_rolling_mean_30" in X.columns

        ):

            X["trend_quantidade"] = (

                X["quantidade_rolling_mean_7"]

                -

                X["quantidade_rolling_mean_30"]

            )

        ###############################################################
        # tendência do faturamento
        ###############################################################

        if (

            "valor_total_rolling_mean_7" in X.columns and

            "valor_total_rolling_mean_30" in X.columns

        ):

            X["trend_valor"] = (

                X["valor_total_rolling_mean_7"]

                -

                X["valor_total_rolling_mean_30"]

            )

        ###############################################################
        # crescimento percentual
        ###############################################################

        if (

            "quantidade_rolling_mean_30" in X.columns

        ):

            X["growth_quantidade"] = np.where(

                X["quantidade_rolling_mean_30"] > 0,

                (

                    X["quantidade_rolling_mean_7"]

                    /

                    X["quantidade_rolling_mean_30"]

                ) - 1,

                0

            )

        ###############################################################
        # substituir infinitos
        ###############################################################

        X.replace(

            [np.inf, -np.inf],

            0,

            inplace=True

        )

        X.fillna(0, inplace=True)
        show_log(f"TrendFeatureCreator aplicado")

        return X

class CategoryEncoder(BaseEstimator, TransformerMixin):
    """
    Label Encoder para múltiplas colunas.

    - Aplica apenas nas colunas especificadas.
    - Converte as colunas para dtype 'category'.
    - Categorias desconhecidas recebem -1.
    """

    def __init__(self, columns=None):

        self.columns = columns or [
            "item_code",
            "filial_id",
            "vendedor_code",
            "item_group",
            "armazem",
            "cliente_uf",
        ]

        self.mappings_ = {}

    def fit(self, X, y=None):

        X = X.copy()

        # Garante que todas as colunas existam
        missing = [c for c in self.columns if c not in X.columns]

        if missing:
            raise ValueError(
                f"As seguintes colunas não existem no DataFrame: {missing}"
            )

        for col in self.columns:

            # Converte para string e depois para category
            valores = (
                X[col]
                .fillna("__MISSING__")
                .astype(str)
                .astype("category")
            )

            categorias = list(valores.cat.categories)

            self.mappings_[col] = {
                categoria: indice
                for indice, categoria in enumerate(categorias)
            }

        return self

    def transform(self, X):

        X = X.copy()

        for col in self.columns:

            valores = (
                X[col]
                .fillna("__MISSING__")
                .astype(str)
                .astype("category")
            )

            X[col] = (
                valores.astype(str)
                .map(self.mappings_[col])
                .fillna(-1)
                .astype("int32")
            )

        show_log("CategoryEncoder aplicado")

        return X

    def inverse_transform(self, X):

        X = X.copy()

        for col in self.columns:

            inv = {
                v: k
                for k, v in self.mappings_[col].items()
            }

            X[col] = (
                X[col]
                .map(inv)
                .astype("category")
            )

        return X

class ZScoreNormalizer(BaseEstimator, TransformerMixin):

    def __init__(self, columns=None):

        self.columns = columns or [
            "preco_unitario",
            "desconto_pct",
            "lucro_bruto",
            "ticket_medio",
            "receita_unitaria",
            "margem_bruta_pct",
            "preco_lag_1",
            "preco_lag_7",
            "preco_lag_30",
            "preco_rolling_mean_7",
            "preco_rolling_mean_30",
            "preco_rolling_std_30",
            "indice_preco",
            "growth_quantidade",
            "trend_quantidade"
        ]

    def fit(self, X, y=None):

        X = X.copy()
        X_numeric = X.select_dtypes(include=[np.number])

        self.columns_ = list(X_numeric.columns)

        self.mean_ = {}
        self.std_ = {}

        for col in self.columns_:

            self.mean_[col] = X[col].mean()

            std = X[col].std()

            # evita divisão por zero
            if std == 0 or np.isnan(std):
                std = 1.0

            self.std_[col] = std

        return self

    def transform(self, X):

        X = X.copy()

        for col in self.columns_:

            X[col] = (X[col] - self.mean_[col]) / self.std_[col]
        
        show_log(f"ZScoreNormalizer aplicado")
        return X



# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def build_preprocessing_pipeline() -> Pipeline:
    """
    Pipeline completo de pré-processamento para previsão de vendas
    utilizando XGBoost.

    Fluxo:

    Raw
        ↓
    SalesAggregator
        ↓
    CalendarCompleter
        ↓
    NullHandler
        ↓
    TemporalFeatureCreator
        ↓
    LagFeatureCreator
        ↓
    RollingFeatureCreator
        ↓
    DemandFeatureCreator
        ↓
    PriceFeatureCreator
        ↓
    BusinessFeatureCreator
        ↓
    TrendFeatureCreator
        ↓
    CategoryEncoder
    """

    pipeline= Pipeline([

    ("sales_aggregator", SalesAggregator()),

    ("calendar_completer", CalendarCompleter()),

    ("null_handler", NullHandler()),

    ("temporal_features", TemporalFeatureCreator()),

    ("lag_features", LagFeatureCreator()),

    ("rolling_features", RollingFeatureCreator()),
    
    ("demand_features", DemandFeatureCreator()),

    ("price_features", PriceFeatureCreator()),

    ("business_features", BusinessFeatureCreator()),

    ("trend_features", TrendFeatureCreator()),

    ("category_encoder", CategoryEncoder()),

    #("zscore_normalizer",ZScoreNormalizer())
    
    ])
    
    return pipeline

# ---------------------------------------------------------------------------
# Split temporal (treino / validação / teste)
# ---------------------------------------------------------------------------


def temporal_split(
    df: pd.DataFrame,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Separação temporal do dataset (sem embaralhar — respeita a ordem cronológica).

    Em séries temporais NÃO se usa random split. O split é feito por data:
    - Treino: dados mais antigos (70%)
    - Validação: dados intermediários (15%)
    - Teste: dados mais recentes (15%)

    Parameters
    ----------
    df : pd.DataFrame
        Dataset com coluna 'data_venda' já como datetime.
    train_ratio : float
        Proporção para treino.
    val_ratio : float
        Proporção para validação.
    test_ratio : float
        Proporção para teste.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        (df_train, df_val, df_test)
    """
    assert abs(train_ratio + val_ratio + test_ratio - 1.0) < 1e-6, "Proporções devem somar 1.0"

    # Ordenar por data
    df = remove_columns(df)
    df = df.sort_values("data_venda").reset_index(drop=True)

    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    df_train = df.iloc[:train_end].copy()
    df_val = df.iloc[train_end:val_end].copy()
    df_test = df.iloc[val_end:].copy()

    show_log(f"Split temporal realizado:")
    show_log(f"  Treino:     {len(df_train):,} registros ({df_train['data_venda'].min()} a {df_train['data_venda'].max()})")
    show_log(f"  Validação:  {len(df_val):,} registros ({df_val['data_venda'].min()} a {df_val['data_venda'].max()})")
    show_log(f"  Teste:      {len(df_test):,} registros ({df_test['data_venda'].min()} a {df_test['data_venda'].max()})")

    return df_train, df_val, df_test


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------


def load_parquet(filepath: str = INPUT_PATH) -> pd.DataFrame:
    """Carrega o dataset em formato Parquet."""
    show_log(f"Carregando dataset de: {filepath}")
    df = pd.read_parquet(filepath)
    show_log(f"Dataset carregado: {df.shape[0]:,} registros, {df.shape[1]} colunas")
    return df


def save_splits(
    df_train: pd.DataFrame,
    df_val: pd.DataFrame,
    df_test: pd.DataFrame,
    output_dir: str = OUTPUT_DIR,
    output_format: str = "parquet",
):
    """Salva os splits em arquivos separados."""
    os.makedirs(output_dir, exist_ok=True)

    for name, df in [("train", df_train), ("val", df_val), ("test", df_test)]:
        if output_format == "parquet":
            filepath = os.path.join(output_dir, f"dataset_{name}.parquet")
            df.to_parquet(filepath, index=False, engine="pyarrow")
        else:
            filepath = os.path.join(output_dir, f"dataset_{name}.csv")
            df.to_csv(filepath, index=False)
        show_log(f"Salvo: {filepath} ({len(df):,} registros)")



def execute_preprocessing_pipeline(input_path, output_path, train_ratio=0.7,val_ratio=0.15, test_ratio=0.15, output_format="parquet"):
    """
    Pipeline completo de preprocessamento:
    1. Lê Parquet
    2. Aplica pipeline (nulos + features temporais)
    3. Split temporal
    4. Salva splits
    """

    show_log("=" * 60)
    show_log("GP Corp - Preprocessing Pipeline (Série Temporal)")
    show_log("=" * 60)

    # 1. Carregar dados
    df = load_parquet(input_path)

    # 2. Aplicar pipeline
    show_log("\nIniciando pipeline de preprocessamento...")
    pipeline = build_preprocessing_pipeline()
    df_processed = pipeline.fit_transform(df)
    show_log(f"\nDataset processado: {df_processed.shape[0]:,} registros, {df_processed.shape[1]} colunas")

    # 3. Split temporal
    show_log("\nRealizando split temporal...")
    df_train, df_val, df_test = temporal_split(
        df_processed,
        train_ratio=train_ratio,
        val_ratio=val_ratio,
        test_ratio=test_ratio,
    )

    # 4. Salvar
    show_log("\nSalvando splits...")
    if output_format == "parquet":
        show_log(f"\nSalvando splits no formato {output_format}...")
        save_splits(df_train, df_val, df_test, output_dir=output_path, output_format=output_format)
    
    elif  output_format == "csv":
        filepath_train = os.path.join(output_path, "dataset_train.csv")
        df_train.to_csv(filepath_train, index=False)
        
        filepath_test = os.path.join(output_path, "dataset_test.csv")
        df_test.to_csv(filepath_train, index=False)
    
        filepath_val = os.path.join(output_path, "dataset_val.csv")
        df_val.to_csv(filepath_train, index=False)
        
    else:
        show_log(f"Formato não suportado! Arquivo não salvo")
        raise

    show_log("\nPreprocessamento concluído.")
    show_log("=" * 60)

    return pipeline


# ---------------------------------------------------------------------------
# Entrypoint principal
# ---------------------------------------------------------------------------
    

def main():
    """
    Pipeline completo de preprocessamento:
    1. Lê Parquet
    2. Aplica pipeline (nulos + features temporais)
    3. Split temporal
    4. Salva splits
    """
    import argparse

    parser = argparse.ArgumentParser(description="Preprocessing pipeline para série temporal")
    parser.add_argument("--input-path", type=str, default=INPUT_PATH, help="Caminho do Parquet de entrada")
    parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="Diretório de saída dos splits")
    parser.add_argument("--train-ratio", type=float, default=0.7, help="Proporção treino")
    parser.add_argument("--val-ratio", type=float, default=0.15, help="Proporção validação")
    parser.add_argument("--test-ratio", type=float, default=0.15, help="Proporção teste")
    parser.add_argument("--output-format", type=str, default="parquet", choices=["parquet", "csv"])
    args = parser.parse_args()

    show_log("=" * 60)
    show_log("GP Corp - Preprocessing Pipeline (Série Temporal)")
    show_log("=" * 60)

    # 1. Carregar dados
    df = load_parquet(args.input_path)

    # 2. Aplicar pipeline
    show_log("\nIniciando pipeline de preprocessamento...")
    pipeline = build_preprocessing_pipeline()
    df_processed = pipeline.fit_transform(df)
    show_log(f"\nDataset processado: {df_processed.shape[0]:,} registros, {df_processed.shape[1]} colunas")

    # 3. Split temporal
    show_log("\nRealizando split temporal...")
    df_train, df_val, df_test = temporal_split(
        df_processed,
        train_ratio=args.train_ratio,
        val_ratio=args.val_ratio,
        test_ratio=args.test_ratio,
    )

    # 4. Salvar
    show_log("\nSalvando splits...")
    save_splits(df_train, df_val, df_test, output_dir=args.output_dir, output_format=args.output_format)

    show_log("\nPreprocessamento concluído.")
    show_log("=" * 60)


if __name__ == "__main__":
    main()
