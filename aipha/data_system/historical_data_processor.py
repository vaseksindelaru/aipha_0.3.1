"""
Módulo para el procesamiento de datos históricos brutos y su almacenamiento.

Este componente toma los archivos de datos descargados por los 'Fetchers',
los transforma en un formato estructurado (DataFrame de pandas) y los carga
en una base de datos persistente (DuckDB) para su posterior análisis.
"""

import io
import logging
import zipfile
from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class HistoricalDataProcessor:
    """
    Procesa archivos de datos históricos (ZIPs), los transforma en
    DataFrames de pandas y los almacena en una base de datos DuckDB.
    """

    def __init__(self, db_path: Path):
        """
        Inicializa el procesador.

        Args:
            db_path (Path): La ruta al archivo de la base de datos DuckDB.
        """
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"HistoricalDataProcessor inicializado para la base de datos: {self.db_path}"
        )

    def _create_tables(self, con: duckdb.DuckDBPyConnection):
        """Crea las tablas 'klines' y 'trades' si no existen."""
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS klines (
                symbol VARCHAR,
                interval VARCHAR,
                open_time TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                close_time TIMESTAMP,
                quote_asset_volume DOUBLE,
                number_of_trades BIGINT,
                taker_buy_base_asset_volume DOUBLE,
                taker_buy_quote_asset_volume DOUBLE,
                PRIMARY KEY (symbol, interval, open_time)
            );
        """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                symbol VARCHAR,
                trade_id BIGINT,
                price DOUBLE,
                qty DOUBLE,
                quote_qty DOUBLE,
                trade_time TIMESTAMP,
                is_buyer_maker BOOLEAN,
                is_best_match BOOLEAN,
                PRIMARY KEY (symbol, trade_id)
            );
        """
        )
        logger.debug("Tablas 'klines' y 'trades' aseguradas en la base de datos.")

    def _parse_klines_dataframe_from_zip(
        self, zip_path: Path
    ) -> Optional[pd.DataFrame]:
        """Parsea un archivo ZIP de klines y lo convierte en un DataFrame."""
        try:
            parts = zip_path.parts
            symbol, interval = parts[-3], parts[-2]

            with zipfile.ZipFile(zip_path, "r") as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as csv_file:
                    df = pd.read_csv(
                        io.TextIOWrapper(csv_file, "utf-8"),
                        header=None,
                        names=[
                            "open_time", "open", "high", "low", "close", "volume",
                            "close_time", "quote_asset_volume", "number_of_trades",
                            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore",
                        ],
                    )

            df.drop(columns=["ignore"], inplace=True)
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
            df[["open", "high", "low", "close", "volume"]] = df[
                ["open", "high", "low", "close", "volume"]
            ].apply(pd.to_numeric, errors="coerce")

            df["symbol"] = symbol
            df["interval"] = interval
            return df
        except (zipfile.BadZipFile, IndexError, KeyError) as e:
            logger.error(f"No se pudo procesar el archivo klines {zip_path}: {e}")
            return None

    def _parse_trades_dataframe_from_zip(
        self, zip_path: Path
    ) -> Optional[pd.DataFrame]:
        """Parsea un archivo ZIP de trades y lo convierte en un DataFrame."""
        try:
            symbol = zip_path.parts[-2]

            with zipfile.ZipFile(zip_path, "r") as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as csv_file:
                    df = pd.read_csv(
                        io.TextIOWrapper(csv_file, "utf-8"),
                        header=None,
                        names=[
                            "trade_id", "price", "qty", "quote_qty", "trade_time",
                            "is_buyer_maker", "is_best_match",
                        ],
                    )

            df["trade_time"] = pd.to_datetime(df["trade_time"], unit="ms")
            df[["price", "qty", "quote_qty"]] = df[
                ["price", "qty", "quote_qty"]
            ].apply(pd.to_numeric, errors="coerce")
            df["is_buyer_maker"] = df["is_buyer_maker"].astype(bool)
            df["is_best_match"] = df["is_best_match"].astype(bool)
            df["symbol"] = symbol
            return df
        except (zipfile.BadZipFile, IndexError, KeyError) as e:
            logger.error(f"No se pudo procesar el archivo de trades {zip_path}: {e}")
            return None

    def process_and_store_files(self, file_paths: List[Path]):
        """
        Procesa una lista de archivos ZIP, los convierte a DataFrames y los
        almacena en la base de datos DuckDB de forma idempotente.
        """
        klines_dfs, trades_dfs = [], []
        for file_path in file_paths:
            if not file_path.exists():
                logger.warning(f"El archivo no existe, se omite: {file_path}")
                continue

            path_str = str(file_path.as_posix())
            if "/klines/" in path_str:
                df = self._parse_klines_dataframe_from_zip(file_path)
                if df is not None and not df.empty:
                    klines_dfs.append(df)
            elif "/trades/" in path_str:
                df = self._parse_trades_dataframe_from_zip(file_path)
                if df is not None and not df.empty:
                    trades_dfs.append(df)

        if not klines_dfs and not trades_dfs:
            logger.info("No hay nuevos datos para procesar y almacenar.")
            return

        with duckdb.connect(database=str(self.db_path), read_only=False) as con:
            self._create_tables(con)
            if klines_dfs:
                combined_df = pd.concat(klines_dfs, ignore_index=True)
                logger.info(f"Insertando {len(combined_df)} registros de klines...")
                con.execute("INSERT INTO klines BY NAME SELECT * FROM combined_df ON CONFLICT DO NOTHING;")
            if trades_dfs:
                combined_df = pd.concat(trades_dfs, ignore_index=True)
                logger.info(f"Insertando {len(combined_df)} registros de trades...")
                con.execute("INSERT INTO trades BY NAME SELECT * FROM combined_df ON CONFLICT DO NOTHING;")
        logger.info("Procesamiento y almacenamiento de datos completado.")