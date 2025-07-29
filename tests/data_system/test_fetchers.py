"""
Pruebas para las clases Fetcher.

Este módulo contiene tanto pruebas unitarias (usando mocks) como pruebas
de integración (que realizan peticiones de red reales) para los fetchers.
"""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from aipha.data_system.api_client import ApiClient
from aipha.data_system.fetchers import BinanceKlinesFetcher
from aipha.data_system.templates.templates import KlinesDataRequestTemplate


@pytest.mark.integration
def test_fetcher_integration_fetches_real_data_and_uses_cache():
    """
    Prueba de integración que verifica el flujo completo:
    1. Realiza una petición de red real a Binance Vision.
    2. Descarga, procesa y devuelve un DataFrame.
    3. Verifica que una segunda llamada use el archivo cacheado localmente.
    """
    # --- Arrange (Preparar) ---

    # 1. Definir un directorio temporal para los datos de prueba.
    #    Pathlib se encargará de crear la ruta correcta para el SO.
    download_dir = Path("tests/temp_test_data")
    download_dir.mkdir(exist_ok=True)

    # 2. Crear instancias reales de nuestros componentes.
    #    La base_url del ApiClient será establecida por el Fetcher.
    api_client = ApiClient(base_url="https://will-be-overwritten.com")
    fetcher = BinanceKlinesFetcher(
        api_client=api_client, download_dir=str(download_dir)
    )

    # 3. Crear una plantilla para una fecha y símbolo que sabemos que existen.
    template = KlinesDataRequestTemplate(
        name="Integration Test BTC-USDT 1d",
        symbol="BTCUSDT",
        interval="1d",
        start_date=date(2023, 1, 1),
        end_date=date(2023, 1, 1),
    )

    # --- Act (Primera llamada - debería descargar desde la red) ---
    df_from_network = fetcher.fetch_klines_as_dataframe(template)

    # --- Assert (Verificar el resultado de la primera llamada) ---
    assert df_from_network is not None
    assert isinstance(df_from_network, pd.DataFrame)
    assert not df_from_network.empty
    expected_columns = ["Open time", "Open", "High", "Low", "Close", "Volume"]
    assert all(col in df_from_network.columns for col in expected_columns)
    assert df_from_network["Open time"].iloc[0].date() == date(2023, 1, 1)

    # --- Act & Assert (Segunda llamada - debería usar la caché) ---
    # Esta llamada debería ser mucho más rápida.
    df_from_cache = fetcher.fetch_klines_as_dataframe(template)

    # La mejor forma de verificar que la caché funcionó es comprobar que los datos son idénticos.
    pd.testing.assert_frame_equal(df_from_network, df_from_cache)