"""
Módulo del Orquestador de Señales.
"""
from pathlib import Path
from typing import Any, Dict
import duckdb
import pandas as pd
from aipha.trading_flow.detectors.accumulation_zone_detector import AccumulationZoneDetector
from aipha.trading_flow.detectors.key_candle_detector import KeyCandleDetector
from aipha.trading_flow.detectors.trend_detector import TrendDetector
from aipha.trading_flow.detectors.signal_combiner import SignalCombiner

class SignalOrchestrator:
    """Coordina la ejecución de múltiples detectores de señales en un pipeline."""
    def __init__(self, db_path: Path, config: Dict[str, Any]):
        if not db_path.exists():
            raise FileNotFoundError(f"La base de datos no se encuentra en: {db_path}")
        self.db_path = db_path
        self.config = config
        
        # Instanciación de todos los detectores del pipeline
        self.accumulation_zone_detector = AccumulationZoneDetector(
            **self.config.get("accumulation_zone", {})
        )
        self.key_candle_detector = KeyCandleDetector()
        self.trend_detector = TrendDetector(**self.config.get("trend", {}))
        self.signal_combiner = SignalCombiner(**self.config.get("signal_combiner", {}))

    def _load_data(self, symbol: str, interval: str) -> pd.DataFrame:
        """Carga los datos de k-lines desde DuckDB de forma segura."""
        with duckdb.connect(database=str(self.db_path), read_only=True) as con:
            query = "SELECT * FROM klines WHERE symbol = ? AND interval = ? ORDER BY open_time;"
            df = con.execute(query, [symbol, interval]).fetchdf()
        if not df.empty:
            df["open_time"] = pd.to_datetime(df["open_time"])
            df["close_time"] = pd.to_datetime(df["close_time"])
        return df

    def generate_signals(self, symbol: str, interval: str) -> pd.DataFrame:
        """
        Ejecuta el pipeline completo de detección de señales:
        1. Zonas de Acumulación
        2. Velas Clave
        3. Tendencias
        4. Combinación de Señales
        """
        df_base = self._load_data(symbol, interval)
        if df_base.empty:
            return df_base

        # Etapa 1: Detectar Zonas de Acumulación
        df_with_zones = self.accumulation_zone_detector.detect(df_base)

        # Etapa 2: Detectar Velas Clave
        df_with_key_candles = self.key_candle_detector.detect(
            df_with_zones, **self.config.get("key_candle", {})
        )
        
        # Etapa 3: Detectar Tendencias
        df_with_trends = self.trend_detector.detect(df_with_key_candles)
        
        # Etapa 4: Combinar Señales para la coincidencia triple
        df_final = self.signal_combiner.detect(df_with_trends)
        
        return df_final
