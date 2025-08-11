# aipha/agents/tools/fetcher_tool.py

from langchain_core.tools import tool
from pydantic.v1 import BaseModel, Field
from typing import List
import logging
from datetime import date, timedelta
from pathlib import Path # Importamos Path para el tipo de retorno

# Importamos las clases necesarias de tu sistema de datos
from aipha.data_system.api_client import ApiClient
from aipha.data_system.fetchers import BinanceVisionFetcher
from aipha.data_system.templates.templates import KlinesDataRequestTemplate

# Configuración básica para poder ver los logs de la herramienta
logging.basicConfig(level=logging.INFO) # Ajusta a INFO para ver los logs de descarga
logger = logging.getLogger(__name__)


# --- 1. Definir el Esquema de Entrada de la Herramienta ---
# Usamos Pydantic para decirle al LLM exactamente qué argumentos necesita
# esta función y de qué tipo son. Esto es crucial para que el LLM
# pueda construir la llamada a la herramienta correctamente.
class FetcherInput(BaseModel):
    symbol: str = Field(description="El símbolo del par de trading, por ejemplo 'BTCUSDT'.")
    interval: str = Field(description="El intervalo de las velas (klines), por ejemplo '1d', '1h', '5m'.")
    days_ago_start: int = Field(description="Número de días en el pasado para el inicio del rango de fechas. Ej: 30 para datos de hace 30 días.")
    days_ago_end: int = Field(description="Número de días en el pasado para el final del rango de fechas. Usa 0 para hoy.")


# --- 2. Crear la Herramienta (la función Python que el agente llamará) ---
@tool(args_schema=FetcherInput)
def fetch_binance_data(symbol: str, interval: str, days_ago_start: int, days_ago_end: int) -> List[Path]: # Tipo de retorno: List[Path]
    """
    Descarga datos históricos de klines (velas) de Binance Vision
    y devuelve una lista de las rutas a los archivos ZIP descargados localmente.
    """
    logger.info(f"Herramienta 'fetch_binance_data' invocada con: symbol={symbol}, interval={interval}, start={days_ago_start}d ago, end={days_ago_end}d ago")

    try:
        # --- Configuración del Sistema de Datos ---
        # Instanciamos ApiClient y BinanceVisionFetcher.
        # `base_url` se establece en el fetcher, el ApiClient solo necesita una instancia.
        api_client = ApiClient(base_url="") 
        # Directorio de descarga alineado con el test
        fetcher = BinanceVisionFetcher(api_client=api_client, download_dir="./temp_test_data")

        # --- Calculamos las fechas a partir de 'days_ago_start' y 'days_ago_end' ---
        today = date.today()
        start_date = today - timedelta(days=days_ago_start)
        end_date = today - timedelta(days=days_ago_end)

        # --- Creación del Template de Datos ---
        template = KlinesDataRequestTemplate(
            name=f"{symbol}_{interval}_data",
            symbol=symbol,
            interval=interval,
            start_date=start_date,
            end_date=end_date
        )

        # --- Ejecución del Fetcher (el corazón de la herramienta) ---
        local_file_paths = fetcher.ensure_data_is_downloaded(template)
        
        # Devolvemos la lista de objetos Path directamente
        return local_file_paths

    except Exception as e:
        logger.error(f"Error ejecutando la herramienta fetch_binance_data: {e}")
        # En caso de error, devolver una lista con un Path que indique el error
        return [Path(f"Error_during_fetch: {e}")]

# Bloque para probar la herramienta directamente (opcional, para depuración manual)
if __name__ == "__main__":
    print("--- Probando fetch_binance_data directamente ---")
    # Descargar klines de BTCUSDT 1d para los últimos 3 días (hoy=0, ayer=1, anteayer=2)
    # Esto creará un directorio 'temp_test_data' en la raíz de tu proyecto.
    test_result = fetch_binance_data(symbol="BTCUSDT", interval="1d", days_ago_start=2, days_ago_end=0)
    print("\nResultados de la descarga:")
    for f in test_result:
        print(f)