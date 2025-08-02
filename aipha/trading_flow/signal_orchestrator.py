from pathlib import Path; from typing import Any, Dict; import duckdb; import pandas as pd
from aipha.trading_flow.detectors.accumulation_zone_detector import AccumulationZoneDetector
from aipha.trading_flow.detectors.key_candle_detector import KeyCandleDetector
from aipha.trading_flow.detectors.trend_detector import TrendDetector
from aipha.trading_flow.detectors.signal_combiner import SignalCombiner
from aipha.trading_flow.detectors.signal_scorer import SignalScorer
class SignalOrchestrator:
    def __init__(self, db_path: Path, config: Dict[str, Any]):
        if not db_path.exists(): raise FileNotFoundError(f"DB not found: {db_path}")
        self.db_path, self.config = db_path, config
        self.zone_detector = AccumulationZoneDetector(**self.config.get("accumulation_zone", {}))
        self.key_candle_detector = KeyCandleDetector()
        self.trend_detector = TrendDetector(**self.config.get("trend", {}))
        self.signal_combiner = SignalCombiner(**self.config.get("signal_combiner", {}))
        self.signal_scorer = SignalScorer()
    def _load_data(self, symbol: str, interval: str) -> pd.DataFrame:
        with duckdb.connect(database=str(self.db_path), read_only=True) as con:
            df = con.execute("SELECT * FROM klines WHERE symbol = ? AND interval = ? ORDER BY open_time;", [symbol, interval]).fetchdf()
        if not df.empty:
            df["open_time"] = pd.to_datetime(df["open_time"]); df["close_time"] = pd.to_datetime(df["close_time"])
        return df
    def generate_signals(self, symbol: str, interval: str) -> pd.DataFrame:
        df = self._load_data(symbol, interval);
        if df.empty: return df
        df = self.zone_detector.detect(df)
        df = self.trend_detector.detect(df) # Tendencia y Zona se pueden calcular en paralelo sobre df
        df = self.key_candle_detector.detect(df, **self.config.get("key_candle", {}))
        df = self.signal_combiner.detect(df)
        df = self.signal_scorer.score(df)
        return df