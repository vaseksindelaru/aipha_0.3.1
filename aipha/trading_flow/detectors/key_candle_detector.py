"""
Módulo para la detección de "velas clave" en datos de k-lines.
"""
import numpy as np
import pandas as pd
from typing import Dict, Any

class KeyCandleDetector:
    """Encapsula la lógica para detectar velas clave en un DataFrame."""
    @staticmethod
    def detect(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df_res = df.copy()
        volume_lookback = kwargs.get('volume_lookback', 20)
        volume_percentile_threshold = kwargs.get('volume_percentile_threshold', 0.90)
        body_percentile_threshold = kwargs.get('body_percentile_threshold', 0.30)

        df_res["volume_threshold"] = (
            df_res["volume"]
            .rolling(window=volume_lookback, min_periods=volume_lookback)
            .quantile(volume_percentile_threshold)
            .shift(1)
        )
        df_res["body_size"] = abs(df_res["close"] - df_res["open"])
        candle_range = df_res["high"] - df_res["low"]
        df_res["body_percentage"] = np.where(
            candle_range > 0, df_res["body_size"] / candle_range, 0
        )
        high_volume_condition = df_res["volume"] > df_res["volume_threshold"]
        indecision_body_condition = df_res["body_percentage"] < body_percentile_threshold
        df_res["is_key_candle"] = (
            (high_volume_condition & indecision_body_condition).fillna(False).astype(bool)
        )
        return df_res