"""
Pruebas de integración para la clase SignalOrchestrator.

Estas pruebas verifican que el orquestador puede conectarse a una base de datos,
extraer datos de k-lines, invocar a los detectores y devolver un DataFrame
enriquecido con las señales correctas.
"""

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from aipha.trading_flow.detectors.key_candle_detector import KeyCandleDetector
from aipha.trading_flow.signal_orchestrator import SignalOrchestrator


@pytest.fixture
def populated_db_path(tmp_path: Path) -> Path:
    """
    Crea una base de datos DuckDB temporal y la puebla con datos de k-lines.

    La base de datos se crea en un directorio temporal gestionado por pytest.
    Los datos generados incluyen una vela diseñada específicamente para ser
    detectada como una "vela clave" por el KeyCandleDetector.

    Args:
        tmp_path (Path): Fixture de pytest que proporciona una ruta de directorio temporal.

    Yields:
        Path: La ruta al archivo de la base de datos poblada.
    """
    db_path = tmp_path / "test_trading_flow.db"
    symbol = "BTCUSDT"
    interval = "1d"
    num_rows = 30

    # --- 1. Generar datos de prueba "aburridos" y deterministas ---
    # Estos datos están diseñados para NO activar el detector.
    # El volumen es bajo y el cuerpo de la vela es consistentemente grande.
    timestamps = pd.to_datetime(
        pd.date_range(start="2023-01-01", periods=num_rows, freq="D")
    )
    np.random.seed(42)
    base_open = 100 + np.random.rand(num_rows).cumsum()
    data = {
        "open_time": timestamps,
        "open": base_open,
        "high": base_open + 10,  # Rango grande y consistente
        "low": base_open - 10,
        "close": base_open + 8,   # Cuerpo grande (80% del rango), no activa la indecisión
        "volume": np.random.uniform(1000, 2000, size=num_rows),  # Volumen bajo
        "symbol": symbol,
        "interval": interval,
    }
    df = pd.DataFrame(data)

    # --- 2. Diseñar una "Vela Clave" súper evidente ---
    # Esta vela está diseñada para ser la ÚNICA que active el detector.
    key_candle_index = 25
    df.loc[key_candle_index, "volume"] = 500000  # Volumen exageradamente alto
    df.loc[key_candle_index, "high"] = 180.0     # Mechas largas (rango grande)
    df.loc[key_candle_index, "low"] = 120.0
    df.loc[key_candle_index, "open"] = 150.0
    df.loc[key_candle_index, "close"] = 150.0    # Cuerpo CERO (indecisión máxima)

    # --- 3. Rellenar columnas restantes requeridas por el esquema de la BD ---
    df["close_time"] = df["open_time"] + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)
    df["quote_asset_volume"] = df["volume"] * df["close"]
    df["number_of_trades"] = np.random.randint(100, 500, size=num_rows)
    df["taker_buy_base_asset_volume"] = df["volume"] / 2
    df["taker_buy_quote_asset_volume"] = df["quote_asset_volume"] / 2

    # --- 4. Debug: Verificar que la fixture genera los datos esperados ---
    # Se llama al detector aquí para asegurar que solo se crea 1 vela clave.
    df_for_check = KeyCandleDetector.detect(df.copy())
    key_candles_in_fixture = df_for_check["is_key_candle"].sum()
    print(f"\n[Fixture Debug] Key candles detected in generated data: {key_candles_in_fixture}")
    assert key_candles_in_fixture == 1, (
        "Fixture data generation is faulty, more or less than 1 key candle created."
    )

    # --- 5. Reordenar columnas para que coincidan con el esquema de la tabla ---
    # Esto previene errores de inserción por desajuste en el orden.
    schema_columns = [
        "symbol", "interval", "open_time", "open", "high", "low", "close",
        "volume", "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"
    ]
    df = df[schema_columns]

    # --- 6. Crear y poblar la base de datos ---
    with duckdb.connect(database=str(db_path)) as con:
        # Usar el mismo esquema que HistoricalDataProcessor
        con.execute("CREATE TABLE klines (symbol VARCHAR, interval VARCHAR, open_time TIMESTAMP, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, close_time TIMESTAMP, quote_asset_volume DOUBLE, number_of_trades BIGINT, taker_buy_base_asset_volume DOUBLE, taker_buy_quote_asset_volume DOUBLE);")
        
        # Usar INSERT BY NAME para hacer la inserción inmune al orden de las columnas.
        con.execute("INSERT INTO klines BY NAME SELECT * FROM df;")

    yield db_path


def test_generate_signals_with_key_candle(populated_db_path: Path):
    """
    Verifica que SignalOrchestrator procesa los datos y detecta
    correctamente una única vela clave predefinida.
    """
    # --- Arrange (Preparar) ---
    # El orquestador necesita la ruta a la base de datos, que la fixture ya proporciona.
    orchestrator = SignalOrchestrator(db_path=populated_db_path)

    # --- Act (Actuar) ---
    result_df = orchestrator.generate_signals(symbol="BTCUSDT", interval="1d")

    # --- Assert (Verificar) ---
    # 1. Verificar que el resultado es válido
    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty, "El DataFrame resultante no debería estar vacío."
    assert len(result_df) == 30, "El DataFrame debería contener todas las filas de la BD."

    # 2. Verificar que las columnas de detección fueron añadidas
    assert "is_key_candle" in result_df.columns
    assert result_df["is_key_candle"].dtype == bool, "La columna 'is_key_candle' debe ser booleana."

    # 3. Verificar que se detectó exactamente una vela clave
    key_candles_found = result_df["is_key_candle"].sum()
    assert key_candles_found == 1, f"Se esperaba 1 vela clave, pero se encontraron {key_candles_found}."

    # 4. (Opcional) Verificar que la vela clave es la que diseñamos
    key_candle_row = result_df.loc[result_df["is_key_candle"]]
    assert key_candle_row.index[0] == 25, "La vela clave detectada no es la que se diseñó en la fila 25."