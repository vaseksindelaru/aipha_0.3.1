# En tests/data_system/test_data_fetching_agent.py

import pytest
from datetime import date
from pathlib import Path

# Importamos la plantilla de datos que usaremos en el test
from aipha.data_system.templates.templates import KlinesDataRequestTemplate

# ⚠ Esta línea importará el agente. Si el archivo o la función no existen, el test fallará aquí.
# Si ya lo creaste en algún momento, el error puede ser distinto, pero seguirá fallando.
from aipha.agents.data_fetching_agent.agent import build_agent


@pytest.fixture
def sample_klines_template():
    """Fixture que proporciona una plantilla de solicitud de klines de ejemplo."""
    return KlinesDataRequestTemplate(
        symbol="BTCUSDT",
        interval="1d",
        start_date=date(2023, 1, 1),
        end_date=date(2023, 1, 2),
        name="BTC Daily Data Test"
    )

def test_data_fetching_agent_returns_local_paths(sample_klines_template):
    """
    Contrato Irrefutable:
    El DataFetchingAgent debe invocar al BinanceVisionFetcher
    y devolver una lista de objetos Path a los archivos ZIP descargados localmente.
    """
    # Construimos el agente (esta función 'build_agent' aún no existe o no es completa)
    agent = build_agent()
    
    # Invocamos el agente con la plantilla de datos
    # Esperamos que el agente procese esto y use el fetcher
    result_state = agent.invoke({"template": sample_klines_template})
    
    # Verificaciones del resultado (el "Veredicto del Bulldozer")
    # El agente debería devolver una lista de rutas de archivo
    assert "files" in result_state, "El estado final del agente debe contener la clave 'files'."
    assert isinstance(result_state["files"], list), "El agente debe devolver una lista de rutas."
    assert all(isinstance(p, Path) for p in result_state["files"]), "Todos los elementos deben ser rutas locales (Path)."
    
    # Para el rango de 2 días (2023-01-01 y 2023-01-02), esperamos 2 archivos.
    assert len(result_state["files"]) == 2, "Debe devolver una ruta por cada día en el rango."

    # Opcional: Verificar que las rutas parezcan correctas (sin descargar aún)
    expected_paths_part = [
        "temp_test_data/klines/BTCUSDT/1d/BTCUSDT-1d-2023-01-01.zip",
        "temp_test_data/klines/BTCUSDT/1d/BTCUSDT-1d-2023-01-02.zip"
    ]
    assert all(str(p).endswith(ep) for p, ep in zip(result_state["files"], expected_paths_part)), \
        "Las rutas de los archivos no coinciden con el formato esperado."

    print("\n--- Test para DataFetchingAgent PASADO (¡temporalmente!) ---")