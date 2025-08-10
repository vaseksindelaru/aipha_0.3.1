import os
from typing import TypedDict, Annotated

from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph
from langgraph.graph.message import add_messages


# --- 1. Definir el Estado del Agente ---
# El estado es la "memoria" del agente. Es un diccionario
# que viaja entre los nodos del grafo. TypedDict lo hace más estructurado.
class AgentState(TypedDict):
    pregunta: str
    respuesta_llm: str

# --- 2. Crear las Herramientas y Nodos ---

# Inicializamos el LLM de Gemini que será el cerebro de nuestro nodo.
# Usamos el modelo 'flash' que es rápido y eficiente para tareas simples.
llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)

# Este es nuestro primer "nodo de trabajo".
# Es una función que recibe el estado, realiza una acción y devuelve 
# las modificaciones al estado.
def call_gemini_node(state: AgentState):
    """Nodo que invoca al LLM de Gemini para responder una pregunta."""
    print("---INVOCANDO A GEMINI---")
    pregunta = state["pregunta"]
    
    # Invocamos al modelo de lenguaje con la pregunta
    respuesta = llm.invoke(pregunta)
    
    # Guardamos la respuesta en el estado para que otros nodos la puedan usar
    return {"respuesta_llm": respuesta.content}

# --- 3. Definir el Grafo (El Plan de Trabajo) ---

# StateGraph es el tipo de grafo más común en LangGraph
workflow = StateGraph(AgentState)

# Añadimos nuestro nodo al grafo, dándole un nombre único.
workflow.add_node("call_gemini", call_gemini_node)

# Definimos cuál es el punto de entrada al grafo. La ejecución siempre comenzará aquí.
workflow.set_entry_point("call_gemini")

# Definimos cuál es el punto final. Como es un grafo simple, termina después de un solo paso.
workflow.set_finish_point("call_gemini")

# "Compilamos" el grafo para crear la aplicación ejecutable.
app = workflow.compile()

# --- 4. Ejecución de prueba (para verificar que el archivo funciona) ---
if __name__ == "__main__":
    print("Ejecutando prueba del Base Agent...")
    
    # El input es un diccionario que coincide con la estructura de nuestro AgentState.
    estado_inicial = {"pregunta": "Explica qué es LangGraph en menos de 20 palabras."}
    
    # 'invoke' ejecuta el grafo desde la entrada hasta la salida.
    resultado_final = app.invoke(estado_inicial)
    
    print("\n---RESULTADO FINAL---")
    print("Pregunta:", estado_inicial['pregunta'])
    print("Respuesta del LLM:", resultado_final["respuesta_llm"])