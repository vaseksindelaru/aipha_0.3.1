"""
Módulo que contiene el motor de etiquetado de eventos de trading,
fiel a la lógica "potenciada" del prototipo Aipha 0.2.
"""
from typing import List, Union

import pandas as pd
import pandas_ta as ta
import numpy as np

class PotentialCaptureEngine:
    """
    Calcula etiquetas para eventos usando la Triple Barrera Potenciada.

    La lógica replica la del prototipo Aipha 0.2:
    1.  Múltiples niveles de toma de ganancias (etiquetas ordinales 1, 2, 3...).
    2.  Barreras dinámicas basadas en ATR.
    3.  Filtro de calidad de Drawdown para pérdidas que casi fueron ganancias.
    """
    def __init__(self, **kwargs):
        self.config = {
            'profit_factors': kwargs.get('profit_factors', [1.0, 2.0, 3.0]),
            'stop_loss_factor': kwargs.get('stop_loss_factor', 1.0),
            'time_limit': kwargs.get('time_limit', 20),
            'drawdown_threshold': kwargs.get('drawdown_threshold', 0.8), # Drawdown del 80% desde el pico invalida
            'atr_period': kwargs.get('atr_period', 14),
        }
        # Asegurarse de que los profit factors estén ordenados
        self.config['profit_factors'] = sorted(self.config['profit_factors'])

    def label_events(self, prices: pd.DataFrame, t_events: pd.Series) -> pd.Series:
        """
        Aplica la lógica de la triple barrera a una serie de eventos.
        """
        required_cols = ["high", "low", "close"]
        if not all(col in prices.columns for col in required_cols):
            raise ValueError(f"'prices' debe contener las columnas: {required_cols}")

        df = prices.copy()
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=self.config['atr_period'])
        
        valid_events = t_events.dropna().unique()
        valid_events = pd.Series(valid_events)[pd.Series(valid_events).isin(df.index)] # Filtrar eventos no en el índice
        
        labels = pd.Series(0, index=valid_events)
        
        for event_idx in valid_events:
            entry_price = df.loc[event_idx, 'close']
            atr_at_event = df.loc[event_idx, 'atr']
            
            if pd.isna(atr_at_event) or atr_at_event == 0:
                continue

            sl_level = entry_price - (atr_at_event * self.config['stop_loss_factor'])
            tp_levels = {
                i + 1: entry_price + (atr_at_event * pf) 
                for i, pf in enumerate(self.config['profit_factors'])
            }

            path_end_loc = min(df.index.get_loc(event_idx) + self.config['time_limit'], len(df) - 1)
            price_path = df.iloc[df.index.get_loc(event_idx) + 1 : path_end_loc + 1]

            label_found = False
            for t_step, row in price_path.iterrows():
                # Comprobar Take Profit (del más alto al más bajo)
                for level, tp_price in sorted(tp_levels.items(), reverse=True):
                    if row['high'] >= tp_price:
                        labels.loc[event_idx] = level
                        label_found = True
                        break # Rompe el bucle de TP, se ha alcanzado el más alto posible en esta vela
                if label_found:
                    break # Rompe el bucle de tiempo, resultado decidido

                # Comprobar Stop Loss
                if row['low'] <= sl_level:
                    # Filtro de Drawdown
                    path_before_sl = df.loc[event_idx:t_step]
                    highest_before_sl = path_before_sl['high'].max()

                    # Calcular riesgo y ganancia potencial no realizada
                    total_risk = entry_price - sl_level
                    unrealized_gain = highest_before_sl - entry_price
                    
                    # El drawdown es la porción de la ganancia no realizada que se perdió
                    drawdown = (highest_before_sl - row['low']) / unrealized_gain if unrealized_gain > 0 else 0

                    if drawdown >= self.config['drawdown_threshold']:
                        labels.loc[event_idx] = 0 # Operación de baja calidad, neutral
                    else:
                        labels.loc[event_idx] = -1 # Pérdida limpia
                    label_found = True
                    break # Rompe el bucle de tiempo, resultado decidido
            
        return labels
    