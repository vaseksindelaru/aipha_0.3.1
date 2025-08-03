"""
Módulo que contiene el motor de etiquetado de eventos de trading.
VERSIÓN FINAL CANÓNICA V7
"""
import pandas as pd
import pandas_ta as ta

class PotentialCaptureEngine:
    def __init__(self, **kwargs):
        self.cfg = {'profit_factors': sorted(kwargs.get('profit_factors', [1., 2.])),
                    'stop_loss_factor': kwargs.get('stop_loss_factor', 1.),
                    'time_limit': kwargs.get('time_limit', 20),
                    'drawdown_threshold': kwargs.get('drawdown_threshold', 0.8),
                    'atr_period': kwargs.get('atr_period', 14)}

    def label_events(self, prices: pd.DataFrame, t_events: pd.Series) -> pd.Series:
        df = prices.copy()
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=self.cfg['atr_period'])
        valid_events = t_events.dropna().unique(); valid_events = pd.Series(valid_events)[pd.Series(valid_events).isin(df.index)]
        labels = pd.Series(0, index=valid_events, dtype=int)
        
        for t0 in valid_events:
            entry_price = df.loc[t0, 'close']; atr_val = df.loc[t0, 'atr']
            if pd.isna(atr_val) or atr_val == 0: continue
            
            sl = entry_price - atr_val * self.cfg['stop_loss_factor']
            tps = {i+1: entry_price + atr_val*pf for i, pf in enumerate(self.cfg['profit_factors'])}

            end_loc = min(df.index.get_loc(t0) + self.cfg['time_limit'] + 1, len(df))
            path = df.iloc[df.index.get_loc(t0) + 1 : end_loc]

            outcome = 0
            for t1, row in path.iterrows():
                highest_tp_hit = 0
                for level, tp_price in sorted(tps.items(), reverse=True):
                    if row['high'] >= tp_price:
                        highest_tp_hit = level
                        break

                if row['low'] <= sl:
                    path_before_sl = df.loc[t0:t1]
                    peak = path_before_sl['high'].max()
                    
                    if peak > entry_price:
                        gain = peak - entry_price
                        dd = (peak - row['low']) / gain
                        outcome = -1 if dd < self.cfg['drawdown_threshold'] else 0
                    else: # Nunca estuvo en ganancias
                        outcome = -1
                    break
                
                if highest_tp_hit > 0:
                    outcome = highest_tp_hit
                    break
            
            labels[t0] = outcome
            
        return labels