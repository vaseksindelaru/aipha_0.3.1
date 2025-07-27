# --- tests/data_system/test_templates.py ---

"""
Pruebas unitarias para las plantillas de solicitud de datos (contratos de datos).

Estas pruebas validan la correcta serialización, deserialización y la lógica
de validación de los modelos Pydantic definidos en el módulo de plantillas.
"""

from datetime import date

import pytest
from aipha.data_system.templates.templates import (
    BaseDataRequestTemplate,
    KlinesDataRequestTemplate,
)


def test_klines_template_serialization_deserialization_roundtrip():
    """
    Valida que una instancia de KlinesDataRequestTemplate puede ser serializada
    a un diccionario y luego deserializada de vuelta a un objeto idéntico.
    """
    # 1. Arrange: Crear una instancia del contrato de datos con datos de ejemplo.
    # El validador de Pydantic se encargará de convertir 'btcusdt' a 'BTCUSDT'.
    original_template = KlinesDataRequestTemplate(
        name="BTC-USDT 1d Klines Enero 2023",
        symbol="btcusdt",
        interval="1d",
        start_date=date(2023, 1, 1),
        end_date=date(2023, 1, 31),
        description="Datos de prueba para el roundtrip.",
    )

    # 2. Act (Serialización): Convertir el objeto a un diccionario.
    serialized_dict = original_template.to_dict()

    # 3. Assert (Verificación de la Serialización):
    assert isinstance(serialized_dict, dict)
    assert serialized_dict["template_type"] == "klines"
    assert serialized_dict["symbol"] == "BTCUSDT"  # Verificar que el validador funcionó
    assert serialized_dict["start_date"] == "2023-01-01"  # Verificar formato ISO
    assert serialized_dict["end_date"] == "2023-01-31"

    # 4. Act (Deserialización): Reconstruir el objeto desde el diccionario.
    # Se usa la clase base para probar el mecanismo de registro polimórfico.
    reconstructed_template = BaseDataRequestTemplate.from_dict(serialized_dict)

    # 5. Assert (Verificación de la Deserialización):
    assert isinstance(reconstructed_template, KlinesDataRequestTemplate)
    assert original_template == reconstructed_template 