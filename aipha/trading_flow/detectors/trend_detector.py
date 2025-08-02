"""
Módulo para la detección de tendencias utilizando el algoritmo ZigZag y regresión lineal.
"""

from typing import Any

import numpy as np
import pandas as pd
import pandas_ta as ta
from scipy.stats import linregress


class TrendDetector:
    """
    Detecta segmentos de tendencia en datos de precios.

    Utiliza el indicador ZigZag para identificar pivotes de mercado (picos y valles)
    y luego aplica una regresión lineal a cada segmento entre pivotes para
    cuantificar la tendencia (pendiente y R²).
    """

    def __init__(self, **kwargs: Any):
        """
        Inicializa el detector de tendencias.

        Args:
            **kwargs: Argumentos de configuración para el indicador ZigZag.
                - deviation (float): El porcentaje de desviación para el ZigZag. Por defecto 5.0.
                - pivot_legs (int): El número de velas a cada lado de un pivote. Por defecto 3.
        """
        self.config = {
            "deviation": kwargs.get("deviation", 5.0),
            "pivot_legs": kwargs.get("pivot_legs", 3),
        }

    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Añade información de tendencia al DataFrame.

        Calcula los pivotes ZigZag y luego, para cada segmento de tendencia,
        calcula la pendiente y el R² y los añade como nuevas columnas.

        Args:
            df (pd.DataFrame): DataFrame de entrada con columnas 'high', 'low', 'close'.

        Returns:
            pd.DataFrame: El DataFrame original enriquecido con las columnas:
                - trend_id: ID único para cada segmento de tendencia.
                - trend_direction: 'alcista' o 'bajista'.
                - trend_slope: La pendiente de la regresión lineal del segmento.
                - trend_r_squared: El valor R² de la regresión lineal.
        """
        if df.empty:
            return df

        df_res = df.copy()

        # 1. Calcular pivotes ZigZag usando pandas-ta de forma vectorizada
        zigzag_series = df_res.ta.zigzag(
            high=df_res["high"],
            low=df_res["low"],
            deviation=self.config["deviation"],
            pivot_legs=self.config["pivot_legs"],
        )

        pivot_indices = sorted(list(set(zigzag_series.dropna().index)))

        # Asegurarse de que el primer y último punto del DF estén en los pivotes
        # para que todos los datos estén cubiertos por un segmento de tendencia.
        if not pivot_indices or pivot_indices[0] != df_res.index[0]:
            pivot_indices.insert(0, df_res.index[0])
        if pivot_indices[-1] != df_res.index[-1]:
            pivot_indices.append(df_res.index[-1])

        # 2. Inicializar las columnas de salida
        for col in ["trend_id", "trend_direction", "trend_slope", "trend_r_squared"]:
            df_res[col] = pd.NA if col != "trend_direction" else ""

        # 3. Iterar sobre los segmentos entre pivotes y calcular la regresión
        for i in range(len(pivot_indices) - 1):
            start_idx, end_idx = pivot_indices[i], pivot_indices[i + 1]
            if start_idx >= end_idx:
                continue

            segment_df = df_res.loc[start_idx:end_idx]
            if len(segment_df) < 2:
                continue

            x, y = np.arange(len(segment_df)), segment_df["close"].values
            slope, _, r_value, _, _ = linregress(x, y)

            df_res.loc[start_idx:end_idx, "trend_id"] = i
            df_res.loc[start_idx:end_idx, "trend_direction"] = "alcista" if slope > 0 else "bajista"
            df_res.loc[start_idx:end_idx, "trend_slope"] = slope
            df_res.loc[start_idx:end_idx, "trend_r_squared"] = r_value**2

        df_res["trend_id"] = df_res["trend_id"].astype("Int64")
        return df_res
