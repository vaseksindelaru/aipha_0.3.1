"""
Módulo para las estructuras de datos del trading_flow.
"""
from dataclasses import dataclass
import pandas as pd

@dataclass
class AccumulationZoneFeatures:
    """Estructura de datos para almacenar las características de una zona detectada."""
    start_idx: int
    end_idx: int
    high: float
    low: float
    volume_avg: float
    vol_total: float
    vwap: float
    poc: float
    mfi: float
    quality_score: float
    datetime_start: pd.Timestamp
    datetime_end: pd.Timestamp