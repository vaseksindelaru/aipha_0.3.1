import pandas as pd; import pandas_ta as ta
class AccumulationZoneDetector:
    def __init__(self, **kwargs):
        self.config = {'atr_period': kwargs.get('atr_period', 14), 'atr_multiplier': kwargs.get('atr_multiplier', 1.5),
                       'min_zone_bars': kwargs.get('min_zone_bars', 5), 'volume_ma_period': kwargs.get('volume_ma_period', 20),
                       'volume_threshold': kwargs.get('volume_threshold', 1.1)}
    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        df_res = df.copy()
        df_res['atr'] = ta.atr(df_res['high'], df_res['low'], df_res['close'], length=self.config['atr_period'])
        df_res['volume_ma'] = df_res['volume'].rolling(window=self.config['volume_ma_period']).mean()
        df_res['in_accumulation_zone'] = False; df_res['zone_id'] = pd.NA
        in_zone = False; zone_start_idx, zone_id_counter = -1, 0; zone_high, zone_low = 0.0, 0.0
        for i in range(len(df_res)):
            candle = df_res.iloc[i]
            if pd.isna(candle['atr']) or pd.isna(candle['volume_ma']): continue
            vol_ok = candle['volume'] >= candle['volume_ma'] * self.config['volume_threshold']
            if in_zone:
                current_high, current_low = max(zone_high, candle['high']), min(zone_low, candle['low'])
                zone_height = current_high - current_low
                max_h = df_res.loc[zone_start_idx, 'atr'] * self.config['atr_multiplier'] if pd.notna(df_res.loc[zone_start_idx, 'atr']) else float('inf')
                if zone_height > max_h:
                    if (i - 1 - zone_start_idx + 1) >= self.config['min_zone_bars']:
                        df_res.loc[zone_start_idx:i-1, 'in_accumulation_zone'] = True
                        df_res.loc[zone_start_idx:i-1, 'zone_id'] = zone_id_counter; zone_id_counter += 1
                    in_zone = False
            if not in_zone and vol_ok:
                in_zone, zone_start_idx, zone_high, zone_low = True, i, candle['high'], candle['low']
        if in_zone and (len(df_res) - zone_start_idx) >= self.config['min_zone_bars']:
            df_res.loc[zone_start_idx:len(df_res)-1, 'in_accumulation_zone'] = True
            df_res.loc[zone_start_idx:len(df_res)-1, 'zone_id'] = zone_id_counter
        df_res.drop(columns=['atr', 'volume_ma'], inplace=True, errors='ignore')
        df_res['zone_id'] = df_res['zone_id'].astype("Int64")
        return df_res