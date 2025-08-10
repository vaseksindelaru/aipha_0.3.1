# fetcher.py
"""
Módulo que contiene las clases encargadas de obtener datos brutos de fuentes externas,
como APIs o archivos. Estas clases, o "Fetchers", actúan como el primer eslabón
en la cadena de adquisición de datos.
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import List

from aipha.data_system.api_client import ApiClient
from aipha.data_system.templates.templates import (
    BaseDataRequestTemplate,
    KlinesDataRequestTemplate,
    TradesDataRequestTemplate,
)

logger = logging.getLogger(__name__)


class BinanceVisionFetcher:
    """
    Descarga archivos de datos desde el repositorio de Binance Vision.

    Esta clase es responsable de tomar una plantilla de datos (klines, trades, etc.),
    construir las URLs correctas para cada día en el rango de fechas, y asegurar
    que los archivos ZIP correspondientes estén descargados en una caché local.

    Su única responsabilidad es la descarga, no el procesamiento del contenido.
    """

    def __init__(
        self,
        api_client: ApiClient,
        download_dir: str,
        binance_vision_base_url: str = "https://data.binance.vision/data/spot/daily/",
    ):
        """
        Inicializa el fetcher de Binance Vision.

        Args:
            api_client (ApiClient): Una instancia de ApiClient para realizar las peticiones.
            download_dir (str): El directorio raíz donde se guardarán los datos cacheados.
            binance_vision_base_url (str): La URL base de Binance Vision para datos diarios.
        """
        self._api_client = api_client
        self.download_dir = Path(download_dir)
        self._api_client.base_url = binance_vision_base_url.rstrip("/")
        logger.info(
            f"BinanceVisionFetcher inicializado. Directorio de caché: {self.download_dir}"
        )

    def _build_endpoint(self, template: BaseDataRequestTemplate, a_date: date) -> str:
        """
        Construye el endpoint de la API para un tipo de dato y día específicos.

        Inspecciona el tipo de plantilla para construir la ruta correcta.

        Args:
            template (BaseDataRequestTemplate): La plantilla que define los datos.
            a_date (date): La fecha específica para la que se construye el endpoint.

        Returns:
            str: El endpoint relativo para la petición a la API.

        Raises:
            TypeError: Si se proporciona un tipo de plantilla no soportado.
        """
        date_str = a_date.strftime("%Y-%m-%d")
        symbol = template.symbol

        if isinstance(template, KlinesDataRequestTemplate):
            interval = template.interval
            # Ej: 'klines/BTCUSDT/1d/BTCUSDT-1d-2023-01-01.zip'
            return f"klines/{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"
        elif isinstance(template, TradesDataRequestTemplate):
            # Ej: 'trades/BTCUSDT/BTCUSDT-trades-2023-01-01.zip'
            return f"trades/{symbol}/{symbol}-trades-{date_str}.zip"
        else:
            raise TypeError(
                f"Tipo de plantilla no soportado por BinanceVisionFetcher: {type(template).__name__}"
            )

    def ensure_data_is_downloaded(
        self, template: BaseDataRequestTemplate
    ) -> List[Path]:
        """
        Asegura que los datos para el template y rango de fechas estén en la caché local.

        Itera día por día, y si un archivo no existe localmente, lo descarga.
        Devuelve una lista de las rutas locales a todos los archivos solicitados.

        Args:
            template (BaseDataRequestTemplate): El contrato de datos que define qué buscar.

        Returns:
            List[Path]: Una lista de objetos Path apuntando a los archivos ZIP locales.
        """
        requested_files: List[Path] = []
        current_date = template.start_date

        while current_date <= template.end_date:
            try:
                endpoint = self._build_endpoint(template, current_date)
                local_zip_path = self.download_dir / endpoint
                requested_files.append(local_zip_path)

                if local_zip_path.exists():
                    logger.debug(f"Usando caché local para {endpoint}")
                else:
                    logger.info(f"Descargando datos para {endpoint}...")
                    response_content = self._api_client.make_request(
                        method="GET", endpoint=endpoint, parse_json=False
                    )

                    if isinstance(response_content, bytes):
                        local_zip_path.parent.mkdir(parents=True, exist_ok=True)
                        local_zip_path.write_bytes(response_content)
                        logger.info(f"Guardado en caché: {local_zip_path}")
                    else:
                        logger.warning(
                            f"No se pudo descargar {endpoint}. Se omite este día."
                        )
            except TypeError as e:
                logger.error(e)
                return []  # Devolver lista vacía si el template no es válido

            current_date += timedelta(days=1)

        logger.info(f"Verificación de datos completada para el template '{template.name}'.")
        return requested_files
