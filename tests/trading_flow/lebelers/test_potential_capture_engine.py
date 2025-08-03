"""
Pruebas unitarias para la clase PotentialCaptureEngine.

Estas pruebas validan que el motor de etiquetado de la triple barrera
mejorada funciona correctamente en diversos escenarios de mercado simulados:
- Alcance de la toma de ganancias (Take Profit).
- Alcance del stop loss (Stop Loss).
- Vencimiento por límite de tiempo (Time Limit).
- Invalidación de una pérdida por alto drawdown (un "casi gano").
"""

import pandas as pd
import pytest

from aipha.trading_flow.labelers.potential_capture_engine import PotentialCaptureEngine


@pytest.fixture
def base_price_data() -> pd.DataFrame:
    """
    Crea un DataFrame de precios base para las pruebas.

    Genera un mercado lateral y predecible con 100 velas. La volatilidad
    (diferencia entre high y low) es constante, lo que permite que el cálculo
    del ATR sea estable y las barreras sean fáciles de predecir en las pruebas.

    Returns:
        pd.DataFrame: Un DataFrame con columnas 'open', 'high', 'low', 'close'
                      y un índice de tipo DatetimeIndex.
    """
    dates = pd.date_range(start="2023-01-01", periods=100, freq="D")
    data = {
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 100.0,
    }
    df = pd.DataFrame(data, index=dates)
    return df


# Define los escenarios de prueba para la triple barrera.
# Cada escenario modifica el futuro de una manera específica para forzar un resultado.
SCENARIOS = [
    (
        "TP 1 Hit",
        {15: {"high": 111}},  # El 'high' en la vela 15 supera la barrera TP de 110.
        1,  # Etiqueta esperada para el primer nivel de profit_factors.
    ),
    (
        "Time Limit",
        {},  # Sin modificaciones, el precio lateral nunca toca las barreras.
        0,  # Etiqueta esperada para el vencimiento por tiempo.
    ),
    (
        "SL Hit (Clean, No Drawdown)",
        {
            # Forzamos que el precio no suba antes de caer para evitar el filtro de drawdown.
            # Modificamos la vela del evento y las siguientes para que 'high' <= entry_price.
            10: {"high": 100.0},
            11: {"high": 100.0},
            12: {"high": 100.0},
            15: {"low": 89.0, "high": 100.0}, # El 'low' toca la barrera SL de 90.
        },
        -1, # Etiqueta esperada para una pérdida limpia.
    ),
    (
        "SL Hit after High Drawdown",
        {
            # El precio sube casi hasta el TP (110), creando una ganancia no realizada.
            15: {"high": 109.0},
            # Luego, el precio se desploma y toca el SL (90).
            16: {"low": 89.0},
        },
        0,  # La pérdida se filtra como "de baja calidad" y se neutraliza a 0.
    ),
]


@pytest.mark.parametrize("scenario_name, modifications, expected_label", SCENARIOS)
def test_triple_barrier_scenarios(
    base_price_data: pd.DataFrame,
    scenario_name: str,
    modifications: dict,
    expected_label: int,
):
    """
    Valida el comportamiento del etiquetador en diferentes escenarios.

    Args:
        base_price_data (pd.DataFrame): La fixture con los datos de precios.
        scenario_name (str): Nombre descriptivo del caso de prueba.
        modifications (dict): Diccionario para alterar los datos de precios futuros.
        expected_label (int): La etiqueta que se espera como resultado.
    """
    # --- Arrange (Preparar) ---
    prices = base_price_data.copy()
    engine = PotentialCaptureEngine(
        profit_factors=[1.0, 2.0], # TP1 @ 110, TP2 @ 120
        stop_loss_factor=1.0,     # SL @ 90
        time_limit=20,            # Barrera vertical en la vela 30
        drawdown_threshold=0.8,   # 80% de drawdown desde el pico invalida
        atr_period=14,
    )

    # El evento de entrada ocurre en la vela 10.
    event_time = prices.index[10]
    t_events = pd.Series([event_time], index=[event_time])

    # Modificar el DataFrame para simular el escenario de prueba.
    # Las claves del diccionario son localizaciones de índice enteras.
    for idx_loc, col_vals in modifications.items():
        target_time = prices.index[idx_loc]
        for col, val in col_vals.items():
            prices.loc[target_time, col] = val

    # --- Act (Actuar) ---
    labels = engine.label_events(prices, t_events)

    # --- Assert (Verificar) ---
    assert not labels.empty, "El resultado de las etiquetas no puede estar vacío."
    actual_label = labels.loc[event_time]
    assert actual_label == expected_label, f"Fallo en el escenario: '{scenario_name}'"