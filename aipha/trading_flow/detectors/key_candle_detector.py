import numpy as np; import pandas as pd
class KeyCandleDetector:
    @staticmethod
    def detect(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df_res = df.copy(); vl, vpt, bpt = kwargs.get('volume_lookback', 20), kwargs.get('volume_percentile_threshold', 0.90), kwargs.get('body_percentile_threshold', 0.30)
        df_res["volume_threshold"] = df_res["volume"].rolling(window=vl, min_periods=vl).quantile(vpt).shift(1)
        df_res["body_size"] = abs(df_res["close"] - df_res["open"]); cr = df_res["high"] - df_res["low"]
        df_res["body_percentage"] = np.where(cr > 0, df_res["body_size"] / cr, 0)
        hvc = df_res["volume"] > df_res["volume_threshold"]; ibc = df_res["body_percentage"] < bpt
        df_res["is_key_candle"] = ((hvc & ibc).fillna(False).astype(bool))
        return df_res