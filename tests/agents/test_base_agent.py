# En tests/agents/test_base_agent.py

import pytest
# Esta importación fallará porque base_agent/agent.py no tiene una función build_agent
# Por ahora, solo tiene la lógica de ejecución directa (__main__)
from aipha.agents.base_agent.agent import app # Importamos directamente la app compilada

def test_base_agent_responds_to_a_question():
    """
    Contrato Irrefutable:
    El base_agent debe ser capaz de procesar una pregunta simple
    y devolver una respuesta del LLM.
    """
    # Definimos la pregunta que le haremos al agente
    pregunta_de_prueba = "¿Cuál es la capital de Francia?"

    # Preparamos el estado inicial del agente con nuestra pregunta
    estado_inicial = {"pregunta": pregunta_de_prueba}

    # Ejecutamos el agente. Esta es la parte que el agente debe hacer bien.
    # Aquí invocamos directamente la 'app' que ya está compilada en agent.py
    resultado_final = app.invoke(estado_inicial)

    # Verificaciones (las "assertions" que definen el éxito del contrato)
    # 1. Aseguramos que la clave 'respuesta_llm' esté en el resultado
    assert "respuesta_llm" in resultado_final, "El agente no devolvió 'respuesta_llm' en su estado final."

    # 2. Aseguramos que la respuesta no esté vacía
    assert len(resultado_final["respuesta_llm"]) > 0, "La respuesta del LLM está vacía."

    # 3. (Opcional, pero bueno para la prueba) Podemos verificar si la respuesta contiene la palabra esperada
    # Nota: Los LLMs pueden variar, así que esta verificación puede ser frágil.
    # Por ahora, nos enfocamos en que devuelva *algo* coherente.
    assert "París" in resultado_final["respuesta_llm"] or "Paris" in resultado_final["respuesta_llm"], \
        f"La respuesta del LLM no menciona la capital esperada. Respuesta: {resultado_final['respuesta_llm']}"

    print(f"\n--- Test para base_agent PASADO ---")
    print(f"Pregunta: {pregunta_de_prueba}")
    print(f"Respuesta del LLM: {resultado_final['respuesta_llm']}")