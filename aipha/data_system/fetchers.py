"""
Módulo que contiene las clases encargadas de obtener datos brutos de fuentes externas,
como APIs o archivos. Estas clases, o "Fetchers", actúan como el primer eslabón
en la cadena de adquisición de datos.
"""

import io
import logging
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd

from aipha.data_system.api_client import ApiClient
from aipha.data_system.templates.templates import KlinesDataRequestTemplate

logger = logging.getLogger(__name__)


class BinanceKlinesFetcher:
    """
    Obtiene datos de velas (Klines) desde el repositorio de datos históricos de Binance Vision.

    Esta clase se encarga de descargar archivos ZIP diarios que contienen los datos de klines
    para un par de símbolos y un intervalo específicos. Implementa una estrategia de caché
    local para evitar descargas repetidas.

    Utiliza un ApiClient para las peticiones HTTP y un KlinesDataRequestTemplate para
    definir los parámetros de la solicitud de datos.
    """

    # Columnas estándar para los archivos CSV de klines de Binance Vision.
    KLINES_COLUMNS = [
        "Open time", "Open", "High", "Low", "Close", "Volume",
        "Close time", "Quote asset volume", "Number of trades",
        "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"
    ]

    def __init__(
        self,
        api_client: ApiClient,
        download_dir: str,
        binance_vision_base_url: str = "https://data.binance.vision/data/spot/daily/klines",
    ):
        """
        Inicializa el fetcher de Klines de Binance.

        Args:
            api_client (ApiClient): Una instancia de ApiClient para realizar las peticiones.
            download_dir (str): El directorio raíz donde se guardarán los datos cacheados.
            binance_vision_base_url (str): La URL base de Binance Vision para klines diarias.
        """
        self._api_client = api_client
        # Usamos pathlib para una gestión de rutas moderna y robusta.
        self.download_dir = Path(download_dir)
        self._api_client.base_url = binance_vision_base_url
        logger.info(
            f"BinanceKlinesFetcher inicializado. Directorio de caché: {self.download_dir}"
        )

    def _build_endpoint(self, symbol: str, interval: str, a_date: date) -> str:
        """Construye el endpoint de la API para un día específico."""
        date_str = a_date.strftime("%Y-%m-%d")
        # Ej: 'BTCUSDT/1d/BTCUSDT-1d-2023-01-01.zip'
        return f"{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"

    def _process_zip_content(self, zip_content: bytes) -> Optional[pd.DataFrame]:
        """
        Procesa el contenido binario de un archivo ZIP para extraer y limpiar los datos del CSV.

        Args:
            zip_content (bytes): El contenido binario del archivo .zip descargado.

        Returns:
            Optional[pd.DataFrame]: Un DataFrame con los datos de klines procesados,
                                    o None si el contenido del ZIP es inválido.
        """
        try:
            # Usamos io.BytesIO para tratar el contenido binario como un archivo en memoria.
            with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
                # Asumimos que cada ZIP contiene un único archivo CSV.
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as csv_file:
                    df = pd.read_csv(csv_file, header=None, names=self.KLINES_COLUMNS)

            # --- Limpieza y tipado de datos ---
            df = df.drop(columns=["Ignore"])
            df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
            df["Close time"] = pd.to_datetime(df["Close time"], unit="ms")

            numeric_cols = ["Open", "High", "Low", "Close", "Volume", "Quote asset volume",
                            "Taker buy base asset volume", "Taker buy quote asset volume"]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
            df["Number of trades"] = df["Number of trades"].astype(int)

            return df

        except (zipfile.BadZipFile, IndexError, KeyError) as e:
            logger.error(f"No se pudo procesar el contenido del archivo ZIP: {e}")
            return None

    def fetch_klines_as_dataframe(
        self, template: KlinesDataRequestTemplate
    ) -> Optional[pd.DataFrame]:
        """
        Obtiene los datos de klines para el rango de fechas especificado en el template.

        Itera día por día, descarga los datos si no están en la caché local,
        los procesa y los concatena en un único DataFrame.

        Args:
            template (KlinesDataRequestTemplate): El contrato de datos que define qué buscar.

        Returns:
            Optional[pd.DataFrame]: Un DataFrame con todos los datos solicitados,
                                    ordenados por fecha, o None si no se encontraron datos.
        """
        all_daily_dfs: List[pd.DataFrame] = []
        current_date = template.start_date

        while current_date <= template.end_date:
            endpoint = self._build_endpoint(template.symbol, template.interval, current_date)
            local_zip_path = self.download_dir / endpoint
            zip_content: Optional[bytes] = None

            if local_zip_path.exists():
                logger.info(f"Usando caché local para {endpoint}")
                zip_content = local_zip_path.read_bytes()
            else:
                logger.info(f"Descargando datos para {endpoint}...")
                # TODO: El método `make_request` del ApiClient actual devuelve JSON.
                # Se necesita un método o parámetro para obtener el contenido binario (`response.content`).
                # Por ahora, asumimos que puede devolver `bytes` para continuar con la lógica.
                response_content = self._api_client.make_request(
                    method="GET",
                    endpoint=endpoint,
                )

                if isinstance(response_content, bytes):
                    zip_content = response_content
                    # Crear directorios padres si no existen y guardar el archivo en caché.
                    local_zip_path.parent.mkdir(parents=True, exist_ok=True)
                    local_zip_path.write_bytes(zip_content)
                    logger.info(f"Guardado en caché: {local_zip_path}")
                else:
                    logger.warning(f"No se pudo descargar el contenido para {endpoint}. Se omite este día.")

            if zip_content:
                daily_df = self._process_zip_content(zip_content)
                if daily_df is not None:
                    all_daily_dfs.append(daily_df)

            current_date += timedelta(days=1)

        if not all_daily_dfs:
            logger.warning(f"No se encontraron datos para el template '{template.name}'")
            return None

        # Concatenar todos los DataFrames diarios en uno solo.
        final_df = pd.concat(all_daily_dfs, ignore_index=True)
        final_df = final_df.sort_values(by="Open time").reset_index(drop=True)

        logger.info(
            f"Se obtuvieron {len(final_df)} klines para el template '{template.name}' "
            f"desde {final_df['Open time'].min()} hasta {final_df['Close time'].max()}"
        )
        return final_df

