"""
Pruebas unitarias para la clase HistoricalDataProcessor.

Estas pruebas verifican que el procesador puede leer correctamente
archivos ZIP, parsear los datos CSV que contienen, y almacenarlos
correctamente en una base de datos DuckDB.
"""

import io
import zipfile
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from aipha.data_system.historical_data_processor import HistoricalDataProcessor


@pytest.fixture
def mock_klines_zip_path(tmp_path: Path) -> Path:
    """
    Crea un archivo ZIP de klines simulado en un directorio temporal.

    La estructura de directorios simula la salida de BinanceVisionFetcher,
    lo que permite al procesador extraer metadatos como 'symbol' e 'interval'.
    """
    # 1. Definir la estructura de directorios y el contenido del CSV
    klines_dir = tmp_path / "klines" / "BTCUSDT" / "1d"
    klines_dir.mkdir(parents=True, exist_ok=True)

    csv_content = (
        "1672617600000,16600.0,16700.0,16500.0,16650.0,1000,1672703999999,16650000,500,500,8325000,0\n"
        "1672704000000,16650.0,16800.0,16600.0,16750.0,1200,1672790399999,20100000,600,600,10050000,0\n"
    )
    csv_filename = "BTCUSDT-1d-2023-01-02.csv"
    csv_path = klines_dir / csv_filename
    csv_path.write_text(csv_content)

    # 2. Comprimir el CSV en un archivo ZIP
    zip_filename = "BTCUSDT-1d-2023-01-02.zip"
    zip_path = klines_dir / zip_filename
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(csv_path, arcname=csv_filename)

    # 3. Devolver la ruta al archivo ZIP
    yield zip_path


def test_process_and_store_klines(mock_klines_zip_path: Path, tmp_path: Path):
    """
    Verifica que el procesador puede procesar un archivo ZIP de klines
    y almacenarlo correctamente en una base de datos DuckDB.
    """
    # --- Arrange (Preparar) ---
    # Usar tmp_path para la base de datos asegura que la prueba esté aislada
    db_path = tmp_path / "test_data.db"
    processor = HistoricalDataProcessor(db_path=db_path)
    file_paths = [mock_klines_zip_path]

    # --- Act (Actuar) ---
    processor.process_and_store_files(file_paths)

    # --- Assert (Verificar) ---
    # Conectarse a la base de datos para verificar los resultados
    assert db_path.exists(), "El archivo de la base de datos no fue creado."
    con = duckdb.connect(database=str(db_path), read_only=True)

    # 1. Verificar el número de filas insertadas
    count_result = con.execute("SELECT COUNT(*) FROM klines").fetchone()
    assert count_result is not None, "La consulta de conteo no devolvió resultados."
    assert count_result[0] == 2, "El número de filas insertadas es incorrecto."

    # 2. Verificar el contenido y los tipos de datos
    result_df = con.execute("SELECT * FROM klines ORDER BY open_time").fetchdf()
    con.close()

    assert not result_df.empty
    assert result_df["symbol"].iloc[0] == "BTCUSDT"
    assert result_df["interval"].iloc[0] == "1d"

    # 3. Verificar los tipos de datos de las columnas clave
    expected_types = {
        "open_time": "datetime64[us]","close_time": "datetime64[us]",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "number_of_trades": "int64",
    }
    for col, expected_type in expected_types.items():
        assert str(result_df[col].dtype) == expected_type, f"La columna '{col}' tiene un tipo incorrecto."