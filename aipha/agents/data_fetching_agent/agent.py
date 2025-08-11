# En aipha/agents/data_fetching_agent/agent.py

import logging
from typing import List, TypedDict
from datetime import date, timedelta

from langgraph.graph import StateGraph, END

# Importamos nuestra herramienta de adquisición de datos
from aipha.agents.tools.fetcher_tool import fetch_binance_data
# Importamos la plantilla de datos que el agente recibirá como entrada
from aipha.data_system.templates.templates import KlinesDataRequestTemplate

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO) # Ajusta a INFO para ver los logs de descarga


# --- 1. Definir el Estado del Agente (La Pizarra Compartida) ---
# Este diccionario define la información que fluirá entre los nodos de nuestro grafo.
class AgentState(TypedDict):
    template: KlinesDataRequestTemplate  # La plantilla de solicitud de datos que el agente procesará
    files: List[str]                   # La lista de rutas a los archivos descargados

# --- 2. Definir los Nodos del Grafo ---

def execute_fetch_tool_node(state: AgentState) -> AgentState:
    """
    Nodo encargado de extraer los parámetros de la plantilla
    y ejecutar la herramienta 'fetch_binance_data'.
    """
    logger.info("DataFetchingAgent: Ejecutando nodo de herramienta de fetch...")
    
    template = state["template"]
    
    # Calculamos los días de inicio y fin a partir de la fecha actual y la plantilla.
    # Esto es necesario porque nuestra herramienta 'fetch_binance_data' espera 'days_ago_start' y 'days_ago_end'.
    today = date.today()
    days_ago_start = (today - template.start_date).days
    days_ago_end = (today - template.end_date).days

    # Llamamos directamente a nuestra herramienta 'fetch_binance_data'.
    # La herramienta se encarga de usar el BinanceVisionFetcher interno.
    downloaded_files = fetch_binance_data(
        symbol=template.symbol,
        interval=template.interval,
        days_ago_start=days_ago_start,
        days_ago_end=days_ago_end
    )
    
    # Actualizamos el estado del agente con las rutas de los archivos descargados.
    state["files"] = downloaded_files
    logger.info(f"DataFetchingAgent: Archivos descargados y añadidos al estado: {downloaded_files}")
    return state

# --- 3. Construir el Grafo (El Plano del Agente) ---

def build_agent():
    """
    Construye y compila el grafo de nuestro DataFetchingAgent.
    """
    workflow = StateGraph(AgentState)

    # Añadimos nuestro único nodo al grafo.
    workflow.add_node("fetch_data_node", execute_fetch_tool_node)

    # Definimos el punto de entrada y salida del grafo.
    # Para este agente simple, el mismo nodo es el inicio y el fin.
    workflow.set_entry_point("fetch_data_node")
    workflow.set_finish_point("fetch_data_node")

    # Compilamos el grafo para crear una aplicación ejecutable.
    app = workflow.compile()
    return app

# --- 4. Bloque de Ejecución de Prueba (Solo cuando se ejecuta directamente) ---
if __name__ == "__main__":
    print("--- Ejecutando DataFetchingAgent para prueba manual ---")
    
    # Creamos una plantilla de ejemplo para la ejecución manual.
    # Esto descargará datos de BTCUSDT 1d para los últimos 3 días (hoy, ayer, anteayer).
    # Los archivos se guardarán en el directorio './temp_download_cache' en la raíz del proyecto.
    sample_template_for_run = KlinesDataRequestTemplate(
        symbol="BTCUSDT",
        interval="1d",
        start_date=date.today() - timedelta(days=2), # Datos de hace 2 días
        end_date=date.today(),                      # Hasta hoy
        name="Manual BTC Daily Data"
    )

    # Invocamos el agente con la plantilla de ejemplo.
    final_state = build_agent().invoke({"template": sample_template_for_run})
    
    print("\n--- Estado Final del DataFetchingAgent (Prueba Manual) ---")
    print(f"Archivos Descargados: {final_state['files']}")