# aipha/agents/data_fetching_agent/agent.py

import logging
from typing import List, TypedDict
from datetime import date, timedelta
from pathlib import Path

from langgraph.graph import StateGraph, END

# Importamos todas las herramientas necesarias
from aipha.agents.tools.fetcher_tool import fetch_binance_data
from aipha.agents.tools.data_processing_tool import process_historical_data
from aipha.agents.tools.data_query_tool import query_klines_open_price

# Importamos la plantilla de datos (para la solicitud inicial)
from aipha.data_system.templates.templates import KlinesDataRequestTemplate

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO) # Ajusta a DEBUG para ver más logs internos


# --- 1. Definir el Estado del Agente (La Pizarra Compartida Expandida) ---
# Este estado ahora contendrá todas las entradas necesarias para la secuencia
# de descarga, procesamiento y consulta, así como los resultados intermedios y finales.
class AgentState(TypedDict):
    # Entradas para la descarga (se derivan de una KlinesDataRequestTemplate inicial)
    fetch_symbol: str
    fetch_interval: str
    fetch_days_ago_start: int
    fetch_days_ago_end: int

    # Entradas para la consulta (específicas para el precio de apertura)
    query_symbol: str
    query_interval: str
    query_timestamp: str # Formato YYYY-MM-DD HH:MM:SS
    query_db_name: str

    # Salidas de las herramientas/nodos
    downloaded_files: List[Path] # Rutas a los archivos ZIP descargados
    processing_status: str       # Mensaje de estado del procesamiento
    query_result: str            # Resultado final de la consulta (el precio o error)


# --- 2. Definir los Nodos del Grafo ---

def execute_fetch_tool_node(state: AgentState) -> AgentState:
    """
    Nodo encargado de ejecutar la herramienta 'fetch_binance_data'
    usando los parámetros de descarga del estado.
    """
    logger.info("DataFetchingAgent: Ejecutando nodo de descarga de datos...")
    
    # Preparamos los argumentos de la herramienta en un diccionario
    tool_arguments = {
        "symbol": state["fetch_symbol"],
        "interval": state["fetch_interval"],
        "days_ago_start": state["fetch_days_ago_start"],
        "days_ago_end": state["fetch_days_ago_end"]
    }
    
    # Llamamos a la herramienta y actualizamos el estado
    downloaded_files = fetch_binance_data.run(tool_arguments)
    
    state["downloaded_files"] = downloaded_files
    logger.info(f"DataFetchingAgent: Archivos descargados: {downloaded_files}")
    return state

def execute_processing_tool_node(state: AgentState) -> AgentState:
    """
    Nodo encargado de ejecutar la herramienta 'process_historical_data'
    usando los archivos descargados y el nombre de la DB del estado.
    """
    logger.info("DataFetchingAgent: Ejecutando nodo de procesamiento de datos...")

    # Preparamos los argumentos de la herramienta
    # Convertimos Path a string para la entrada de la herramienta (definida así por Pydantic)
    file_paths_str = [str(p) for p in state["downloaded_files"]]
    tool_arguments = {
        "file_paths": file_paths_str,
        "db_name": state["query_db_name"]
    }

    # Llamamos a la herramienta y actualizamos el estado
    processing_msg = process_historical_data.run(tool_arguments)
    
    state["processing_status"] = processing_msg
    logger.info(f"DataFetchingAgent: Estado del procesamiento: {processing_msg}")
    return state

def execute_query_tool_node(state: AgentState) -> AgentState:
    """
    Nodo encargado de ejecutar la herramienta 'query_klines_open_price'
    usando los parámetros de consulta del estado.
    """
    logger.info("DataFetchingAgent: Ejecutando nodo de consulta de datos...")

    # Preparamos los argumentos de la herramienta
    tool_arguments = {
        "symbol": state["query_symbol"],
        "interval": state["query_interval"],
        "timestamp": state["query_timestamp"],
        "db_name": state["query_db_name"]
    }

    # Llamamos a la herramienta y actualizamos el estado
    query_result_str = query_klines_open_price.run(tool_arguments)
    
    state["query_result"] = query_result_str
    logger.info(f"DataFetchingAgent: Resultado de la consulta: {query_result_str}")
    return state


# --- 3. Construir el Grafo (El Plano del Agente con Flujo Multi-Paso) ---

def build_agent():
    """
    Construye y compila el grafo de nuestro DataFetchingAgent.
    Ahora incluye descarga, procesamiento y consulta.
    """
    workflow = StateGraph(AgentState)

    # Añadimos todos nuestros nodos al grafo
    workflow.add_node("fetch_data", execute_fetch_tool_node)
    workflow.add_node("process_data", execute_processing_tool_node)
    workflow.add_node("query_data", execute_query_tool_node)

    # Definimos el flujo secuencial
    workflow.set_entry_point("fetch_data")
    workflow.add_edge("fetch_data", "process_data")
    workflow.add_edge("process_data", "query_data")
    workflow.set_finish_point("query_data") # El grafo termina después de la consulta

    # Compilamos el grafo
    app = workflow.compile()
    return app

# --- 4. Bloque de Ejecución de Prueba (Solo cuando se ejecuta directamente) ---
if __name__ == "__main__":
    print("--- Ejecutando DataFetchingAgent para prueba manual (Descarga, Procesa, Consulta) ---")
    
    # Definimos los parámetros para la descarga y la consulta
    # Ajusta estas fechas y horas según los datos que esperas descargar y consultar
    # Para el ejemplo, usaremos BTCUSDT 1d para el 1 y 2 de Enero de 2023.
    # Luego consultaremos el precio de apertura del 1 de Enero a las 08:00:00 (hora de apertura del día)
    
    # Parámetros de Descarga
    FETCH_SYMBOL = "BTCUSDT"
    FETCH_INTERVAL = "1d"
    FETCH_START_DATE = date(2023, 1, 1)
    FETCH_END_DATE = date(2023, 1, 2) # Descargará 2 días: 2023-01-01 y 2023-01-02

    # Parámetros de Consulta
    QUERY_SYMBOL = "BTCUSDT"
    QUERY_INTERVAL = "1d"
    # La hora de apertura de una vela diaria (1d) suele ser 00:00:00 UTC.
    # Sin embargo, el archivo CSV de Binance Vision almacena el 'open_time' en milisegundos UTC.
    # Cuando se convierte a datetime, si no se especifica timezone, puede interpretarse local.
    # Para ser precisos, buscaremos el timestamp exacto de la apertura del día.
    # Los klines de Binance tienen open_time a las 00:00:00 UTC del día.
    QUERY_TIMESTAMP = "2023-01-01 00:00:00" # Importante: debe coincidir con el timestamp exacto de apertura del kline

    DB_NAME = "aipha_test_data.duckdb" # Nombre de la base de datos para este test

    # Calculamos days_ago_start y days_ago_end para la herramienta fetcher
    today = date.today()
    fetch_days_ago_start = (today - FETCH_START_DATE).days
    fetch_days_ago_end = (today - FETCH_END_DATE).days

    # El estado inicial que se pasa al agente
    initial_state = {
        "fetch_symbol": FETCH_SYMBOL,
        "fetch_interval": FETCH_INTERVAL,
        "fetch_days_ago_start": fetch_days_ago_start,
        "fetch_days_ago_end": fetch_days_ago_end,
        "query_symbol": QUERY_SYMBOL,
        "query_interval": QUERY_INTERVAL,
        "query_timestamp": QUERY_TIMESTAMP,
        "query_db_name": DB_NAME
    }

    # Construimos y ejecutamos el agente
    agent = build_agent()
    final_state = agent.invoke(initial_state)
    
    print("\n--- Estado Final del DataFetchingAgent (Prueba Manual) ---")
    print(f"Archivos Descargados: {final_state['downloaded_files']}")
    print(f"Estado del Procesamiento: {final_state['processing_status']}")
    print(f"Resultado de la Consulta: {final_state['query_result']}")