import pandas as pd; import numpy as np
class SignalScorer:
    @staticmethod
    def _normalize(v, min_v, max_v): return max(0., min(1., (v-min_v)/(max_v-min_v))) if max_v != min_v else 0.5
    @staticmethod
    def score(df: pd.DataFrame) -> pd.DataFrame:
        df_res = df.copy(); df_res['final_score'] = np.nan
        signals = df_res[df_res['is_triple_coincidence']].index
        for idx in signals:
            row = df_res.loc[idx]
            zone_slice = df_res[df_res['zone_id'] == row['zone_id']]
            zone_score = SignalScorer._normalize(len(zone_slice), 5, 50) if not zone_slice.empty else 0.
            trend_score = row.get('trend_r_squared', 0.)
            df_res.loc[idx, 'final_score'] = (zone_score * 0.5) + (trend_score * 0.5)
        return df_res