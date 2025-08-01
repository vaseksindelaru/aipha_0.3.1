"""
Módulo para la detección de zonas de acumulación/distribución.
"""
import pandas as pd
import numpy as np
import pandas_ta as ta
from typing import Dict, Any

class AccumulationZoneDetector:
    """Detecta Zonas de Acumulación basándose en volumen y volatilidad (ATR)."""
    def __init__(self, **kwargs):
        self.config = {
            'atr_period': kwargs.get('atr_period', 14),
            'atr_multiplier': kwargs.get('atr_multiplier', 1.5),
            'min_zone_bars': kwargs.get('min_zone_bars', 5),
            'volume_ma_period': kwargs.get('volume_ma_period', 20),
            'volume_threshold': kwargs.get('volume_threshold', 1.1),
        }

    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        df_res = df.copy()
        
        # Pre-cálculo de indicadores
        df_res['atr'] = ta.atr(df_res['high'], df_res['low'], df_res['close'], length=self.config['atr_period'])
        df_res['volume_ma'] = df_res['volume'].rolling(
            window=self.config['volume_ma_period'], min_periods=self.config['volume_ma_period']
        ).mean()

        df_res['in_accumulation_zone'] = False
        df_res['zone_id'] = pd.NA
        
        in_zone = False
        zone_start_idx = -1
        zone_id_counter = 0
        zone_high = 0.0
        zone_low = 0.0

        for i in range(len(df_res)):
            candle = df_res.iloc[i]
            # No podemos operar si los indicadores no están listos
            if pd.isna(candle['atr']) or pd.isna(candle['volume_ma']):
                continue

            volume_condition_met = candle['volume'] > candle['volume_ma'] * self.config['volume_threshold']

            if in_zone:
                # Estando en una zona, verificar si la vela actual rompe los límites
                current_high = max(zone_high, candle['high'])
                current_low = min(zone_low, candle['low'])
                zone_height = current_high - current_low
                # Usar el ATR del INICIO de la zona como referencia de volatilidad estable
                max_height_allowed = df_res.loc[zone_start_idx, 'atr'] * self.config['atr_multiplier']

                if zone_height > max_height_allowed: # Si el precio se escapa, la zona termina
                    zone_end_idx = i - 1
                    if (zone_end_idx - zone_start_idx + 1) >= self.config['min_zone_bars']:
                        df_res.loc[zone_start_idx:zone_end_idx, 'in_accumulation_zone'] = True
                        df_res.loc[zone_start_idx:zone_end_idx, 'zone_id'] = zone_id_counter
                        zone_id_counter += 1
                    
                    in_zone = False
                    zone_start_idx = -1
                else: # Si el precio se mantiene, la zona continúa
                    zone_high = current_high
                    zone_low = current_low

            if not in_zone and volume_condition_met:
                # Iniciar una nueva zona si no estamos en una y se cumple la condición de volumen
                in_zone = True
                zone_start_idx = i
                zone_high = candle['high']
                zone_low = candle['low']

        # Asegurarse de cerrar cualquier zona que llegue hasta el final del DataFrame
        if in_zone and (len(df_res) - 1 - zone_start_idx + 1) >= self.config['min_zone_bars']:
            df_res.loc[zone_start_idx:len(df_res)-1, 'in_accumulation_zone'] = True
            df_res.loc[zone_start_idx:len(df_res)-1, 'zone_id'] = zone_id_counter
        
        df_res['zone_id'] = df_res['zone_id'].astype("Int64")
        
        # Eliminar columnas intermedias para mantener el resultado limpio
        df_res.drop(columns=['atr', 'volume_ma'], inplace=True, errors='ignore')

        return df_res