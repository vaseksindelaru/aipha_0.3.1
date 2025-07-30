"""
Módulo que contiene el orquestador de señales de trading.

Este componente es responsable de coordinar el flujo de trabajo para la
generación de señales, desde la obtención de datos de la base de datos
hasta la aplicación de diversos detectores.
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd

from aipha.trading_flow.detectors.key_candle_detector import KeyCandleDetector

logger = logging.getLogger(__name__)


class SignalOrchestrator:
    """
    Orquesta el pipeline de generación de señales de trading.

    Esta clase se encarga de:
    1. Cargar los datos de k-lines necesarios desde una base de datos DuckDB.
    2. Invocar a los detectores de patrones (ej. KeyCandleDetector) para
       enriquecer los datos.
    3. Devolver un DataFrame con las señales detectadas.
    """

    def __init__(self, db_path: Path):
        """
        Inicializa el orquestador de señales.

        Args:
            db_path (Path): La ruta al archivo de la base de datos DuckDB.
        """
        self.db_path = db_path
        if not self.db_path.exists():
            raise FileNotFoundError(f"La base de datos no se encuentra en: {self.db_path}")
        logger.info(f"SignalOrchestrator inicializado con la base de datos: {self.db_path}")

    def generate_signals(self, symbol: str, interval: str) -> pd.DataFrame:
        """
        Ejecuta el pipeline de detección de señales para un par y un intervalo dados.

        Args:
            symbol (str): El símbolo del par de trading (ej. 'BTCUSDT').
            interval (str): El intervalo de las velas (ej. '1d', '4h').

        Returns:
            pd.DataFrame: Un DataFrame que contiene los datos de k-lines originales
                          enriquecidos con las columnas de detección de señales.
                          Devuelve un DataFrame vacío si no se encuentran datos.
        """
        logger.info(f"Iniciando generación de señales para {symbol} en intervalo {interval}...")

        try:
            with duckdb.connect(database=str(self.db_path), read_only=True) as con:
                # 1. Construir y ejecutar la consulta SQL de forma segura
                query = "SELECT * FROM klines WHERE symbol = ? AND interval = ? ORDER BY open_time;"
                df = con.execute(query, [symbol, interval]).fetchdf()

            # 2. Validación de Datos
            if df.empty:
                logger.warning(
                    f"No se encontraron datos de klines para {symbol}/{interval}. "
                    "Devolviendo DataFrame vacío."
                )
                return df

            logger.info(f"Cargados {len(df)} registros de klines. Aplicando detectores...")

            # 3. Invocar al Detector
            df_with_signals = KeyCandleDetector.detect(df)

            key_candles_found = df_with_signals["is_key_candle"].sum()
            logger.info(f"Detección completada. Se encontraron {key_candles_found} velas clave.")

            return df_with_signals

        except duckdb.Error as e:
            logger.error(f"Error de DuckDB al generar señales para {symbol}/{interval}: {e}")
            return pd.DataFrame()

