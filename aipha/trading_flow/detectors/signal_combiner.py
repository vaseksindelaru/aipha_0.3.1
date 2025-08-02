import pandas as pd


class SignalCombiner:
    """
    Combina señales de diferentes detectores buscando proximidad temporal.
    La lógica no es una simple intersección, sino que busca una Zona de Acumulación
    en una ventana de tiempo reciente antes de una Vela Clave que ocurra
    durante una tendencia de alta calidad.
    """

    def __init__(self, tolerance: int = 5, min_r_squared: float = 0.8):
        """
        Inicializa el combinador de señales.
        Args:
            tolerance (int): Número de velas hacia atrás desde la vela clave
                             para buscar una zona de acumulación. Por defecto 5.
            min_r_squared (float): El valor R² mínimo para considerar una
                                     tendencia como válida. Por defecto 0.8.
        """
        self.tolerance = tolerance
        self.min_r_squared = min_r_squared

    def detect(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detecta "señales de triple coincidencia" basadas en la proximidad de eventos.
        Una señal se genera en una Vela Clave si:
        1. Ocurre durante una tendencia con un R² suficientemente alto.
        2. Una zona de acumulación ha estado activa en las `tolerance` velas anteriores.
        Args:
            df (pd.DataFrame): DataFrame que contiene las columnas de señales de los
                               detectores anteriores ('is_key_candle',
                               'in_accumulation_zone', 'trend_r_squared').
        Returns:
            pd.DataFrame: El DataFrame enriquecido con la columna 'is_triple_coincidence'.
        """
        df_res = df.copy()

        # Inicializa la nueva columna de salida.
        df_res['is_triple_coincidence'] = False

        # Encuentra los índices de todas las Velas Clave.
        key_candle_indices = df_res[df_res['is_key_candle']].index

        for idx in key_candle_indices:
            # Define una "ventana de búsqueda" hacia atrás.
            start_window = max(0, idx - self.tolerance)
            
            # Verificación de Zona: Comprueba si alguna de las velas en esta ventana de búsqueda tiene in_accumulation_zone == True.
            zone_nearby = df_res.loc[start_window:idx, 'in_accumulation_zone'].any()

            # Verificación de Tendencia: Comprueba que la vela clave misma tiene un trend_r_squared que supera el umbral.
            quality_trend = df_res.loc[idx, 'trend_r_squared'] >= self.min_r_squared

            # Si AMBAS condiciones se cumplen, se marca la Vela Clave como una triple coincidencia.
            if zone_nearby and quality_trend:
                df_res.loc[idx, 'is_triple_coincidence'] = True

        return df_res