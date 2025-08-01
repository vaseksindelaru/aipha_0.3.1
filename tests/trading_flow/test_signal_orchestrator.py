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
    
    # 1. Generar DataFrame "Dorado" determinista de 50 filas
    num_rows = 50
    data = {'open_time': pd.to_datetime(pd.date_range(start="2023-01-01", periods=num_rows, freq="h")),
            'open': 10000.0, 'high': 10005.0, 'low': 9995.0,
            'close': 10002.0, 'volume': np.full(num_rows, 100.0)}
    golden_df = pd.DataFrame(data)
    
    # 2. Diseñar una Zona de Acumulación (índices 20 a 28)
    zone_start, zone_end = 20, 28
    golden_df.loc[zone_start:zone_end, 'high'] = 10010.0
    golden_df.loc[zone_start:zone_end, 'low'] = 10000.0
    golden_df.loc[zone_start:zone_end, 'volume'] = 150.0
    
    # 3. Diseñar una Vela Clave (índice 35)
    key_candle_idx = 35
    golden_df.loc[key_candle_idx, 'volume'] = 5000.0
    golden_df.loc[key_candle_idx, 'open'] = 10020.0; golden_df.loc[key_candle_idx, 'close'] = 10020.1
    golden_df.loc[key_candle_idx, 'high'] = 10030.0; golden_df.loc[key_candle_idx, 'low'] = 10010.0

    golden_df["symbol"] = "GOLD-BTC"; golden_df["interval"] = "1h"
    golden_df["close_time"] = golden_df["open_time"] + pd.Timedelta(minutes=59)
    for col in ['quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']: golden_df[col] = 0.0
    
    processor = HistoricalDataProcessor(db_path=db_path)
    with duckdb.connect(database=str(db_path)) as con:
        processor._create_tables(con)
        table_columns = processor._create_tables.__annotations__.get('return', None)
        table_columns = [ 'symbol', 'interval', 'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
        golden_df_ordered = golden_df[table_columns]
        con.execute("INSERT INTO klines BY NAME SELECT * FROM golden_df_ordered")
    return db_path

def test_orchestrator_pipeline_with_golden_data(populated_db_path: Path):
    config = {
        "key_candle": { "volume_lookback": 20, "volume_percentile_threshold": 0.90, "body_percentile_threshold": 0.2 },
        "accumulation_zone": { "min_zone_bars": 5, "atr_multiplier": 1.5, "volume_threshold": 1.1, "volume_ma_period": 10 }
    }
    orchestrator = SignalOrchestrator(db_path=populated_db_path, config=config)
    
    result_df = orchestrator.generate_signals(symbol="GOLD-BTC", interval="1h")
    
    # Assert
    assert result_df["is_key_candle"].sum() == 1, "Debería detectarse una única Vela Clave"
    assert result_df.loc[35, "is_key_candle"] == True, "La Vela Clave no está en el índice esperado"
    
    assert result_df["in_accumulation_zone"].any(), "Debería detectarse al menos una Zona de Acumulación"
    zone_slice = result_df.loc[20:28]
    assert zone_slice["in_accumulation_zone"].all(), "La zona diseñada no fue detectada completamente"
    assert result_df["zone_id"].nunique(dropna=True) == 1, "Debería haber un único ID de zona"
    assert zone_slice["zone_id"].nunique() == 1, "La zona diseñada debería tener un único ID"