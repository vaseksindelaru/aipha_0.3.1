from dataclasses import dataclass
import pandas as pd

@dataclass
class AccumulationZoneFeatures:
    start_idx: int; end_idx: int; high: float; low: float; volume_avg: float
    vol_total: float; vwap: float; poc: float; mfi: float; quality_score: float
    datetime_start: pd.Timestamp; datetime_end: pd.Timestamp