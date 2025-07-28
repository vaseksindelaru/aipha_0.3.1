# --- aipha/data_system/api_client.py (VERSIÓN 2.2 FINAL APROBADA) ---

import logging
import time
from typing import Any, Dict, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class ApiClient:
    """
    Un cliente API genérico y robusto. (versión 2.2)
    Puede devolver tanto datos JSON parseados como contenido binario crudo.
    Maneja el error 404 (Not Found) como un caso esperado (devuelve None) en lugar de un error.
    """
    def __init__(
        self,
        base_url: str,
        default_headers: Optional[Dict[str, str]] = None,
        default_timeout: int = 10,
        total_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self.base_url = base_url.rstrip('/')
        self.default_timeout = default_timeout
        self.session = self._init_session(total_retries, backoff_factor)

        if default_headers:
            self.session.headers.update(default_headers)

        logger.info(f"ApiClient inicializado para la URL base: {self.base_url}")

    def _init_session(self, total_retries: int, backoff_factor: float) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def make_request(
        self,
        method: str,
        endpoint: str,
        parse_json: bool = True,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Optional[Union[Any, bytes]]:
        """
        Realiza una petición HTTP a un endpoint.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        start_time = time.time()
        logger.debug(f"Petición -> {method.upper()} {url} (parse_json={parse_json})")

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

            if response.status_code == 404:
                logger.warning(f"Recurso no encontrado (404) en {url}. Tratado como dato ausente.")
                return None
            
            response.raise_for_status()
            
            if parse_json:
                return response.json()
            else:
                return response.content

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