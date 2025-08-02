"""
Pruebas de integración para la clase SignalOrchestrator.
"""
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
import pytest
from aipha.data_system.historical_data_processor import HistoricalDataProcessor
from aipha.trading_flow.signal_orchestrator import SignalOrchestrator

@pytest.fixture
def populated_db_path(tmp_path: Path) -> Path:
    db_path = tmp_path / "golden_master.db"
    
    # 1. Generar DataFrame "Dorado" determinista de 60 filas
    num_rows = 60
    base_price = 10000.0
    data = {
        'open_time': pd.to_datetime(pd.date_range(start="2023-01-01", periods=num_rows, freq="h")),
        'open': np.full(num_rows, base_price),
        'high': np.full(num_rows, base_price + 5),
        'low': np.full(num_rows, base_price - 5),
        'close': np.full(num_rows, base_price + 2),
        'volume': np.full(num_rows, 100.0)
    }
    golden_df = pd.DataFrame(data)

    # 2. Diseñar una Mini-Tendencia Alcista (índices 20 a 40)
    trend_start, trend_end = 20, 40
    price_increase = np.linspace(0, 50, trend_end - trend_start + 1)
    golden_df.loc[trend_start:trend_end, 'close'] += price_increase

    # 3. Diseñar una Zona de Acumulación (índices 30 a 38)
    zone_start, zone_end = 30, 38
    golden_df.loc[zone_start:zone_end, 'high'] = golden_df.loc[zone_start:zone_end, 'close'] + 2
    golden_df.loc[zone_start:zone_end, 'low'] = golden_df.loc[zone_start:zone_end, 'close'] - 2
    golden_df.loc[zone_start:zone_end, 'volume'] = 150.0

    # 4. Diseñar una Vela Clave (índice 39)
    key_candle_idx = 39
    golden_df.loc[key_candle_idx, 'volume'] = 5000.0
    golden_df.loc[key_candle_idx, 'high'] = golden_df.loc[key_candle_idx, 'close'] + 10
    golden_df.loc[key_candle_idx, 'low'] = golden_df.loc[key_candle_idx, 'close'] - 1

    # Columnas adicionales requeridas
    golden_df["symbol"] = "GOLD-BTC"; golden_df["interval"] = "1h"
    golden_df["close_time"] = golden_df["open_time"] + pd.Timedelta(minutes=59)
    for col in ['quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']: 
        golden_df[col] = 0.0
    
    processor = HistoricalDataProcessor(db_path=db_path)
    with duckdb.connect(database=str(db_path)) as con:
        processor._create_tables(con)
        table_columns = [ 'symbol', 'interval', 'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
        golden_df_ordered = golden_df[table_columns]
        con.execute("INSERT INTO klines BY NAME SELECT * FROM golden_df_ordered")
    return db_path

def test_orchestrator_full_pipeline_detects_triple_coincidence(populated_db_path: Path):
    """
    Verifica que el pipeline completo detecta una señal de triple coincidencia
    en el escenario "dorado" diseñado.
    """
    # Arrange: Configuración para que coincida con el escenario diseñado
    config = {
        "accumulation_zone": { "min_zone_bars": 5, "atr_multiplier": 2.0, "volume_threshold": 1.1, "volume_ma_period": 10 },
        "key_candle": { "volume_lookback": 20, "volume_percentile_threshold": 0.95, "body_percentile_threshold": 0.1 },
        "trend": { "lookback": 20, "min_r_squared": 0.85 },
        "signal_combiner": { "tolerance": 5, "min_r_squared": 0.85 }
    }
    orchestrator = SignalOrchestrator(db_path=populated_db_path, config=config)
    
    # Act: Ejecutar el pipeline completo
    result_df = orchestrator.generate_signals(symbol="GOLD-BTC", interval="1h")
    
    # Assert: Verificar la señal de triple coincidencia
    assert 'is_triple_coincidence' in result_df.columns, "La columna de triple coincidencia no fue creada."
    
    # La señal solo debe ocurrir en el índice de la Vela Clave
    assert result_df.loc[39, 'is_triple_coincidence'] == True, "La señal de triple coincidencia no se detectó en el índice esperado (39)."
    
    # No debe haber otras señales de triple coincidencia
    assert result_df['is_triple_coincidence'].sum() == 1, "Se detectó un número incorrecto de señales de triple coincidencia."
