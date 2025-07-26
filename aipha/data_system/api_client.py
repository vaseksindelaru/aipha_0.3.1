# --- aipha/data_system/api_client.py ---

import logging
import time
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuración del logger para este módulo.
# Otras partes de la aplicación podrán capturar sus logs.
logger = logging.getLogger(__name__)


class ApiClient:
    """
    Un cliente API genérico y robusto para interactuar con endpoints HTTP.

    Esta clase proporciona una interfaz para realizar peticiones HTTP (GET, POST, etc.)
    con reintentos automáticos para errores transitorios del servidor (5xx) y
    limitación de velocidad (429), gestión de timeouts y manejo de errores estructurado.
    Utiliza una sesión de `requests` para la reutilización de conexiones, mejorando
    significativamente el rendimiento en peticiones sucesivas.
    """

    def __init__(
        self,
        base_url: str,
        default_headers: Optional[Dict[str, str]] = None,
        default_timeout: int = 10,
        total_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        """
        Inicializa el ApiClient.

        Args:
            base_url (str): La URL base del API. Ej: 'https://api.binance.com/api/v3'
            default_headers (Optional[Dict[str, str]]): Cabeceras por defecto para
                                                        todas las peticiones.
            default_timeout (int): Timeout en segundos para las peticiones.
            total_retries (int): Número total de reintentos a realizar si la petición falla.
            backoff_factor (float): Factor de backoff para calcular el tiempo de espera
                                    entre reintentos. Fórmula: backoff * (2 ** (retry-1)).
        """
        self.base_url = base_url.rstrip('/')
        self.default_timeout = default_timeout
        self.session = self._init_session(total_retries, backoff_factor)

        if default_headers:
            self.session.headers.update(default_headers)

        logger.info(f"ApiClient inicializado para la URL base: {self.base_url}")


    def _init_session(self, total_retries: int, backoff_factor: float) -> requests.Session:
        """Configura y devuelve una sesión de requests con una estrategia de reintentos."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504], # Reintenta en 'Too Many Requests' y errores de servidor
            allowed_methods=["HEAD", "GET", "POST", "OPTIONS"], # Métodos a reintentar
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session


    def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        """
        Realiza una petición HTTP a un endpoint y devuelve la respuesta parseada como JSON.

        Args:
            method (str): El método HTTP (ej: 'GET', 'POST').
            endpoint (str): El endpoint del API, ej: '/klines'.
            params (Optional[Dict[str, Any]]): Parámetros de la URL (query string).
            data (Optional[Dict[str, Any]]): Datos para enviar en el cuerpo (form-encoded).
            json_data (Optional[Dict[str, Any]]): Datos para enviar en el cuerpo (JSON).
            headers (Optional[Dict[str, Any]]): Cabeceras adicionales para esta petición específica.

        Returns:
            Optional[Any]: Los datos de la respuesta en formato JSON si es exitosa, 
                           sino devuelve None.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        start_time = time.time()
        logger.debug(f"Petición -> {method.upper()} {url} con params: {params}")

        try:
            response = self.session.request(
                method=method.upper(),
                url=url,
                params=params,
                data=data,
                json=json_data,
                headers=request_headers,
                timeout=self.default_timeout,
            )
            response.raise_for_status()  # Lanza HTTPError para respuestas 4xx/5xx
            
            # Devuelve directamente el JSON, que es lo que el 99% de las veces necesitamos
            return response.json()

        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout en la petición a {url}: {e}")
            return None
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Error de conexión a {url}: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error HTTP {e.response.status_code} para {url}: {e.response.text[:200]}...")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error inesperado de requests para {url}: {e}", exc_info=True)
            return None
        finally:
            end_time = time.time()
            logger.debug(f"Petición <- finalizada en {end_time - start_time:.2f} segundos.")
            
    
    def close_session(self):
        """Cierra la sesión de requests para liberar recursos."""
        logger.info("Cerrando la sesión del ApiClient.")
        self.session.close()