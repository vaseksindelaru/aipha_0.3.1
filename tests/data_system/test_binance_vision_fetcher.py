"""
Pruebas para la clase BinanceVisionFetcher.

Este módulo contiene tanto pruebas unitarias (usando mocks) como pruebas
de integración (que realizan peticiones de red reales) para los fetchers.
"""

from datetime import date
from pathlib import Path
from typing import Any, Dict, Type

import pytest

from aipha.data_system.api_client import ApiClient
from aipha.data_system.fetchers import BinanceVisionFetcher
from aipha.data_system.templates.templates import (
    BaseDataRequestTemplate,
    KlinesDataRequestTemplate,
    TradesDataRequestTemplate,
)


@pytest.mark.integration
@pytest.mark.parametrize(
    "template_class, template_args, expected_path_str",
    [
        (
            KlinesDataRequestTemplate,
            {"interval": "1d"},
            "klines/BTCUSDT/1d/BTCUSDT-1d-2025-04-22.zip",
        ),
        (
            TradesDataRequestTemplate,
            {},
            "trades/BTCUSDT/BTCUSDT-trades-2025-04-22.zip",
        ),
    ],
)
def test_fetcher_integration_handles_multiple_data_types(
    template_class: Type[BaseDataRequestTemplate],
    template_args: Dict[str, Any],
    expected_path_str: str,
):
    """
    Prueba de integración parametrizada que verifica que BinanceVisionFetcher
    puede descargar correctamente diferentes tipos de datos (klines, trades, depth).
    """
    # --- Arrange (Preparar) ---
    download_dir = Path("tests/temp_test_data")
    download_dir.mkdir(exist_ok=True)

    api_client = ApiClient(base_url="https://will-be-overwritten.com")
    fetcher = BinanceVisionFetcher(api_client=api_client, download_dir=str(download_dir))

    template = template_class(
        name=f"Integration Test {template_class.__name__}",
        symbol="BTCUSDT",
        start_date=date(2025, 4, 22),
        end_date=date(2025, 4, 22),
        **template_args,
    )

    # --- Act (Actuar) ---
    downloaded_paths = fetcher.ensure_data_is_downloaded(template)

    # --- Assert (Verificar) ---
    assert len(downloaded_paths) == 1, "Debería haber devuelto una ruta de archivo."
    file_path = downloaded_paths[0]

    assert isinstance(file_path, Path), "El elemento devuelto debe ser un objeto Path."
    assert str(file_path).endswith(expected_path_str), "La ruta del archivo no es la esperada."
    assert file_path.exists(), f"El archivo {file_path} no fue descargado al disco."
    assert file_path.is_file(), "La ruta debe apuntar a un archivo, no a un directorio."