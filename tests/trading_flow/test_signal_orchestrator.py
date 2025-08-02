"""Pruebas de integración para la clase SignalOrchestrator."""
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
import pytest
from aipha.data_system.historical_data_processor import HistoricalDataProcessor
from aipha.trading_flow.signal_orchestrator import SignalOrchestrator

@pytest.fixture
def populated_db_path(tmp_path: Path) -> Path:
    db_path = tmp_path / "golden.db"
    
    # 1. GENERACIÓN 100% DETERMINISTA
    num_rows = 50
    timestamps = pd.to_datetime(pd.date_range(start="2023-01-01", periods=num_rows, freq="h"))
    base_df = pd.DataFrame(index=pd.RangeIndex(start=0, stop=num_rows, step=1))
    
    # Valores base
    base_df['open_time'] = timestamps
    base_df['open'] = 10000.0; base_df['high'] = 10005.0
    base_df['low'] = 9995.0; base_df['close'] = 10002.0
    base_df['volume'] = 100.0

    # 2. ESCENARIO "GOLDEN"
    
    # -- TENDENCIA (Índices 20-38) -- PERFECTAMENTE LINEAL
    trend_start, trend_end = 20, 38
    for i in range(trend_start, trend_end + 1):
        price = 10050.0 + (i - trend_start) * 2.0
        base_df.loc[i, ['open', 'high', 'low', 'close']] = price, price + 2.0, price - 2.0, price
        
    # -- ZONA (Índices 30-38) -- DENTRO DE LA TENDENCIA
    zone_start, zone_end = 30, 38
    base_df.loc[zone_start:zone_end, 'high'] = base_df.loc[zone_start, 'high'] + 1.0
    base_df.loc[zone_start:zone_end, 'low'] = base_df.loc[zone_start, 'low'] - 1.0
    base_df.loc[zone_start:zone_end, 'volume'] = 150.0
    
    # -- VELA CLAVE (Índice 39) --
    key_idx = 39
    base_df.loc[key_idx, ['volume', 'open', 'close', 'high', 'low']] = [5000.0, 10088.0, 10088.1, 10098.0, 10078.0]
    
    golden_df = base_df
    golden_df["symbol"] = "GOLD-BTC"; golden_df["interval"] = "1h"
    golden_df["close_time"] = golden_df["open_time"] + pd.Timedelta(minutes=59)
    for col in ['quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']:
        golden_df[col] = 0.0
    
    proc = HistoricalDataProcessor(db_path=db_path)
    with duckdb.connect(database=str(db_path)) as con:
        proc._create_tables(con);
        con.register('df_temp', golden_df)
        con.execute("INSERT INTO klines BY NAME SELECT * FROM df_temp")
    return db_path

def test_orchestrator_full_pipeline_detects_triple_coincidence(populated_db_path: Path):
    """Verifica que el pipeline completo detecta la señal "dorada" diseñada."""
    config = {
        'key_candle': {'volume_lookback': 20, 'volume_percentile_threshold': 0.90, 'body_percentile_threshold': 0.15},
        'accumulation_zone': {'min_zone_bars': 5, 'atr_multiplier': 1.5, 'volume_threshold': 1.2, 'volume_ma_period': 15},
        'trend': {'zigzag_threshold': 0.1},
        'signal_combiner': {'tolerance': 8, 'min_r_squared': 0.20}
    }
    orchestrator = SignalOrchestrator(db_path=populated_db_path, config=config)
    
    result_df = orchestrator.generate_signals(symbol="GOLD-BTC", interval="1h")
    
    key_candle_idx = 39
    
    assert result_df.loc[key_candle_idx, 'is_triple_coincidence'] == True, "La Triple Coincidencia final falló"
    