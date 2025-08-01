"""
Módulo del Orquestador de Señales.
"""
from pathlib import Path
from typing import Any, Dict
import duckdb
import pandas as pd
from aipha.trading_flow.detectors.accumulation_zone_detector import AccumulationZoneDetector
from aipha.trading_flow.detectors.key_candle_detector import KeyCandleDetector

class SignalOrchestrator:
    """Coordina la ejecución de múltiples detectores de señales en un pipeline."""
    def __init__(self, db_path: Path, config: Dict[str, Any]):
        if not db_path.exists():
            raise FileNotFoundError(f"La base de datos no se encuentra en: {db_path}")
        self.db_path = db_path
        self.config = config
        self.key_candle_detector = KeyCandleDetector()
        self.accumulation_zone_detector = AccumulationZoneDetector(
            **self.config.get("accumulation_zone", {})
        )

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
        Ejecuta el pipeline de detección de señales: Zona de Acumulación -> Vela Clave.
        """
        df_base = self._load_data(symbol, interval)
        if df_base.empty:
            return df_base

        # Etapa 1: Detectar Zonas de Acumulación
        df_with_zones = self.accumulation_zone_detector.detect(df_base)

        # Etapa 2: Detectar Velas Clave
        df_final = self.key_candle_detector.detect(
            df_with_zones, **self.config.get("key_candle", {})
        )
        
        return df_final