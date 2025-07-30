"""
Módulo para la detección de "velas clave" en datos de k-lines.

Una "vela clave" se define como una vela que muestra un volumen significativamente
alto junto con un cuerpo pequeño, lo que indica una posible indecisión o un
punto de inflexión en el mercado.
"""

import numpy as np
import pandas as pd


class KeyCandleDetector:
    """
    Encapsula la lógica para detectar velas clave en un DataFrame de k-lines.

    Esta clase proporciona un método estático para identificar velas que cumplen
    con criterios específicos de volumen y tamaño del cuerpo, marcándolas para
    su posterior análisis en el flujo de trading.
    """

    @staticmethod
    def detect(
        df: pd.DataFrame,
        volume_lookback: int = 20,
        volume_percentile_threshold: float = 0.90,
        body_percentile_threshold: float = 0.30,
    ) -> pd.DataFrame:
        """
        Detecta velas clave basadas en el volumen y el tamaño del cuerpo.

        Una vela se considera "clave" si cumple tres condiciones:
        1. Su volumen está por encima de un percentil alto en una ventana retrospectiva.
        2. El tamaño de su cuerpo (open-close) es pequeño en relación con su rango
           total (high-low), indicando indecisión.
        3. Es la primera vela en un grupo de candidatas consecutivas, para evitar
           agrupar señales muy cercanas.

        Args:
            df (pd.DataFrame): DataFrame de k-lines con columnas 'open', 'high',
                               'low', 'close', 'volume'.
            volume_lookback (int): El número de períodos hacia atrás para calcular
                                   el percentil del volumen.
            volume_percentile_threshold (float): El percentil de volumen (0-1) que
                                                 debe superarse.
            body_percentile_threshold (float): El umbral superior (0-1) para el
                                               tamaño relativo del cuerpo de la vela.

        Returns:
            pd.DataFrame: El DataFrame original enriquecido con las siguientes columnas:
                - volume_threshold: El umbral de volumen calculado para cada vela.
                - body_size: El tamaño absoluto del cuerpo de la vela.
                - body_percentage: El tamaño del cuerpo como porcentaje del rango total.
                - is_key_candle: Booleano que indica si la vela es una vela clave.
        """
        # 1. Calcular el umbral de volumen dinámico usando una ventana móvil.
        # Se usa shift(1) para que el cálculo de la vela actual no se incluya a sí misma.
        df["volume_threshold"] = (
            df["volume"]
            .rolling(window=volume_lookback, min_periods=volume_lookback)
            .quantile(volume_percentile_threshold)
            .shift(1)
        )

        # 2. Calcular el tamaño y el porcentaje del cuerpo de la vela
        df["body_size"] = abs(df["close"] - df["open"])
        candle_range = df["high"] - df["low"]
        # Evitar división por cero en velas sin rango (high == low)
        df["body_percentage"] = np.where(
            candle_range > 0, df["body_size"] / candle_range, 0
        )

        # 3. Identificar las velas que cumplen con las condiciones de volumen e indecisión
        high_volume_condition = df["volume"] > df["volume_threshold"]
        indecision_body_condition = df["body_percentage"] < body_percentile_threshold
        is_potential_key_candle = high_volume_condition & indecision_body_condition

        # 4. Filtrar para evitar agrupar velas clave consecutivas.
        # Se marca como True solo la primera vela de un grupo de candidatas.
        is_first_in_group = ~is_potential_key_candle.shift(1).fillna(False)
        df["is_key_candle"] = is_potential_key_candle & is_first_in_group

        return df