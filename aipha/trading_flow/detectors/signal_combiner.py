from typing import Any, Dict; import pandas as pd
class SignalCombiner:
    def __init__(self, **kwargs: Any):
        self.config = {"tolerance": kwargs.get("tolerance", 8), "min_r_squared": kwargs.get("min_r_squared", 0.45)}
    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        df_res = df.copy(); df_res['is_triple_coincidence'] = False
        key_indices = df_res[df_res['is_key_candle']].index
        for idx in key_indices:
            start, end = max(0, idx - self.config["tolerance"]), idx
            window = df_res.loc[start:end]
            zone_nearby = window['in_accumulation_zone'].any()
            candle = df_res.loc[idx]
            trend_ok = pd.notna(candle['trend_r_squared']) and candle['trend_r_squared'] >= self.config["min_r_squared"]
            if zone_nearby and trend_ok: df_res.loc[idx, 'is_triple_coincidence'] = True
        return df_res