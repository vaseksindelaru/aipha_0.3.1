# aipha/agents/tools/data_query_tool.py

from langchain_core.tools import tool
from pydantic.v1 import BaseModel, Field
import logging
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# --- 1. Definir el Esquema de Entrada de la Herramienta ---
class KlinesQueryInput(BaseModel):
    symbol: str = Field(description="El símbolo del par de trading, por ejemplo 'BTCUSDT'.")
    interval: str = Field(description="El intervalo de las velas (klines), por ejemplo '1d', '1h', '5m'.")
    timestamp: str = Field(description="La marca de tiempo exacta (formato YYYY-MM-DD HH:MM:SS) para la que se busca el precio de apertura.")
    db_name: str = Field(description="Nombre del archivo de la base de datos DuckDB a consultar. Ej: 'aipha_data.duckdb'.")


# --- 2. Crear la Herramienta `query_klines_open_price` ---
@tool(args_schema=KlinesQueryInput)
def query_klines_open_price(symbol: str, interval: str, timestamp: str, db_name: str) -> str:
    """
    Consulta la base de datos DuckDB para obtener el precio de apertura (open price)
    de un kline específico (símbolo, intervalo y marca de tiempo exacta).
    Devuelve el precio de apertura como una cadena o un mensaje de "no encontrado".
    """
    logger.info(f"Herramienta 'query_klines_open_price' invocada para {symbol}, {interval} en {timestamp} desde {db_name}.")

    try:
        # Conectamos a la base de datos
        db_path = Path("./temp_test_data/db") / db_name
        if not db_path.exists():
            return f"Error: La base de datos {db_name} no existe en {db_path}."

        with duckdb.connect(database=str(db_path), read_only=True) as con:
            # Convertimos el timestamp a un formato que DuckDB entienda
            # Aseguramos que la columna 'open_time' se compare correctamente.
            query = f"""
            SELECT open
            FROM klines
            WHERE symbol = '{symbol}'
              AND interval = '{interval}'
              AND open_time = '{timestamp}'
            LIMIT 1;
            """
            
            result = con.execute(query).fetchdf()

            if not result.empty:
                open_price = result['open'].iloc[0]
                return f"El precio de apertura para {symbol} ({interval}) en {timestamp} es: {open_price}"
            else:
                return f"No se encontró kline para {symbol} ({interval}) en {timestamp}."

    except Exception as e:
        logger.error(f"Error ejecutando la herramienta query_klines_open_price: {e}")
        return f"Error al consultar el precio de apertura: {e}"

# Bloque para prueba manual de la herramienta
if __name__ == "__main__":
    print("--- Probando query_klines_open_price directamente ---")
    # NOTA: Para que esto funcione, primero debes haber descargado y procesado
    # datos en 'test_klines.duckdb' que incluyan la fecha y hora de la consulta.
    
    # Ejemplo de consulta:
    query_result = query_klines_open_price(
        symbol="BTCUSDT",
        interval="1d",
        timestamp="2023-01-01 08:00:00", # Ajusta este timestamp según tus datos reales
        db_name="test_klines.duckdb"
    )
    print("\nResultado de la consulta:", query_result)