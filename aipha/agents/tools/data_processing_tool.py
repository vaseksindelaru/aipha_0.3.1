# aipha/agents/tools/data_processing_tool.py

from langchain_core.tools import tool
from pydantic.v1 import BaseModel, Field
from typing import List
import logging
from pathlib import Path

# Importamos tu procesador de datos
from aipha.data_system.historical_data_processor import HistoricalDataProcessor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# --- 1. Definir el Esquema de Entrada de la Herramienta ---
class DataProcessorInput(BaseModel):
    file_paths: List[str] = Field(
        description="Lista de rutas de archivos ZIP locales (como strings) a procesar y almacenar."
    )
    db_name: str = Field(
        description="Nombre del archivo de la base de datos DuckDB donde se almacenarán los datos. Ej: 'aipha_data.duckdb'."
    )


# --- 2. Crear la Herramienta `process_historical_data` ---
@tool(args_schema=DataProcessorInput)
def process_historical_data(file_paths: List[str], db_name: str) -> str:
    """
    Procesa una lista de archivos ZIP de datos históricos (klines/trades),
    los convierte en DataFrames y los almacena en una base de datos DuckDB.
    Devuelve un mensaje de confirmación.
    """
    logger.info(f"Herramienta 'process_historical_data' invocada para {len(file_paths)} archivos en {db_name}.")

    try:
        # Convertimos las rutas de string a Path
        paths_to_process = [Path(p) for p in file_paths]
        
        # Ruta de la base de datos (dentro de un directorio 'db' en temp_test_data)
        db_path = Path("./temp_test_data/db") / db_name
        processor = HistoricalDataProcessor(db_path=db_path)
        
        processor.process_and_store_files(paths_to_process)
        
        return f"Datos procesados y almacenados exitosamente en {db_path}. Total de archivos: {len(file_paths)}"
    
    except Exception as e:
        logger.error(f"Error ejecutando la herramienta process_historical_data: {e}")
        return f"Error al procesar y almacenar datos: {e}"

# Bloque para prueba manual de la herramienta
if __name__ == "__main__":
    print("--- Probando process_historical_data directamente (requiere archivos ZIP) ---")
    # Asegúrate de tener algunos archivos ZIP en './temp_test_data/klines/...'
    # para que esta prueba manual funcione.
    
    # Ejemplo de rutas (ajusta según los archivos que realmente tengas después de una descarga)
    dummy_files = [
        "./temp_test_data/klines/BTCUSDT/1d/BTCUSDT-1d-2023-01-01.zip",
        "./temp_test_data/klines/BTCUSDT/1d/BTCUSDT-1d-2023-01-02.zip",
    ]
    
    # Asegúrate de que estos archivos existen o el procesador los ignorará.
    # Puedes ejecutar primero el DataFetchingAgent para generarlos.

    result = process_historical_data(file_paths=dummy_files, db_name="test_klines.duckdb")
    print("\nResultado del procesamiento:", result)