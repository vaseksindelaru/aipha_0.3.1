"""
Pruebas unitarias para la clase PotentialCaptureEngine.
"""
import pandas as pd
import pytest
from aipha.trading_flow.labelers.potential_capture_engine import PotentialCaptureEngine

@pytest.fixture
def base_price_data() -> pd.DataFrame:
    """Crea un DataFrame de precios base para las pruebas."""
    dates = pd.date_range(start="2023-01-01", periods=100, freq="D")
    data = {"open": 100.0, "high": 105.0, "low": 95.0, "close": 100.0}
    df = pd.DataFrame(data, index=dates)
    return df

# Define los escenarios de prueba para la triple barrera.
SCENARIOS = [
    ("TP 1 Hit", {15: {"high": 111.0}}, 1),
    ("TP 2 Hit", {15: {"high": 121.0}}, 2),
    ("Time Limit Hit", {}, 0),
    (
        "SL Hit Clean",
        {   # --- ESCENARIO CORREGIDO ---
            # Forzar que el precio NUNCA suba por encima de la entrada
            11: {"high": 100.0},
            12: {"high": 100.0},
            13: {"high": 100.0},
            14: {"high": 100.0},
            15: {"low": 89.0, "high": 100.0}, # Finalmente toca el SL
        },
        -1,
    ),
    (
        "SL Hit After High Drawdown",
        {
            15: {"high": 109.0}, # Sube casi al TP
            16: {"low": 89.0},  # Luego cae al SL
        },
        0, # El drawdown lo convierte en neutral
    ),
]

@pytest.mark.parametrize("scenario_name, modifications, expected_label", SCENARIOS)
def test_triple_barrier_scenarios(
    base_price_data: pd.DataFrame,
    scenario_name: str,
    modifications: dict,
    expected_label: int,
):
    """Valida el comportamiento del etiquetador en diferentes escenarios."""
    prices = base_price_data.copy()
    engine = PotentialCaptureEngine(
        profit_factors=[1.0, 2.0], stop_loss_factor=1.0, time_limit=20,
        drawdown_threshold=0.8, atr_period=10 # Usar atr_period=10 para estabilidad en test
    )
    
    event_time = prices.index[10]
    t_events = pd.Series([event_time])

    # Modificar el DataFrame para simular el escenario de prueba.
    for idx_loc, col_vals in modifications.items():
        for col, val in col_vals.items():
            # Usar iloc para una asignación más robusta en pruebas
            prices.iloc[idx_loc, prices.columns.get_loc(col)] = val
    
    labels = engine.label_events(prices, t_events)
    
    assert not labels.empty
    actual_label = labels.iloc[0] # Usar iloc[0] porque el índice puede variar
    assert actual_label == expected_label, f"Fallo en el escenario: '{scenario_name}'"