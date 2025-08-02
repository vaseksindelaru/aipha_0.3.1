from typing import Any, List; import numpy as np; import pandas as pd; from scipy.stats import linregress
class TrendDetector:
    def __init__(self, **kwargs: Any): self.config = {"zigzag_threshold": kwargs.get("zigzag_threshold", 0.5)}
    def _detect_zigzag_pivots(self, series: pd.Series) -> List[int]:
        pivots, trend, last_pivot_val, last_pivot_idx = [series.index[0]], None, series.iloc[0], series.index[0]
        for idx, val in series.items():
            if trend is None:
                if abs(val / last_pivot_val - 1) * 100 >= self.config["zigzag_threshold"]: trend = 'up' if val > last_pivot_val else 'down'
            elif trend == 'up':
                if val < last_pivot_val: trend, pivots = 'down', pivots + [last_pivot_idx]
                last_pivot_val, last_pivot_idx = max(last_pivot_val, val), idx if val >= last_pivot_val else last_pivot_idx
            elif trend == 'down':
                if val > last_pivot_val: trend, pivots = 'up', pivots + [last_pivot_idx]
                last_pivot_val, last_pivot_idx = min(last_pivot_val, val), idx if val <= last_pivot_val else last_pivot_idx
        if series.index[-1] not in pivots: pivots.append(series.index[-1])
        return sorted(list(set(pivots)))
    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        df_res = df.copy()
        if len(df_res) < 2: return df_res
        pivot_indices = self._detect_zigzag_pivots(df_res["close"])
        for col in ["trend_id", "trend_direction", "trend_slope", "trend_r_squared"]: df_res[col] = pd.NA if col != "trend_direction" else ""
        for i in range(len(pivot_indices) - 1):
            start, end = pivot_indices[i], pivot_indices[i+1]
            if start >= end: continue
            segment = df_res.loc[start:end]; x, y = np.arange(len(segment)), segment["close"].values
            if len(segment) < 2: continue
            slope, _, r_val, _, _ = linregress(x, y)
            df_res.loc[start:end, ["trend_id", "trend_direction", "trend_slope", "trend_r_squared"]] = i, "alcista" if slope > 0 else "bajista", slope, r_val**2
        df_res["trend_id"] = df_res["trend_id"].astype("Int64")
        return df_res