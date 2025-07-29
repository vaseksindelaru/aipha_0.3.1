# --- aipha/data_system/templates/templates.py ---

"""
Módulo para definir las plantillas de solicitud de datos (contratos de datos)
utilizando Pydantic para una validación robusta.

Este sistema reemplaza la implementación anterior basada en dataclasses,
aprovechando las capacidades de validación y serialización de Pydantic.

Incluye un mecanismo de registro polimórfico que permite deserializar
diccionarios a la clase de plantilla correcta basándose en un campo 'template_type'.
"""

import abc
from datetime import date
from typing import Any, ClassVar, Dict, Literal, Optional, Type

from pydantic import BaseModel, field_validator, model_validator

# Registro para la deserialización polimórfica.
# Almacena un mapeo de 'template_type' a su clase correspondiente.
_template_registry: Dict[str, Type["BaseDataRequestTemplate"]] = {}


def register_template(template_type: str):
    """
    Decorador para registrar una clase de plantilla en el registro global.

    Args:
        template_type (str): El identificador único para el tipo de plantilla.

    Returns:
        Callable: El decorador que registra la clase.

    Raises:
        ValueError: Si el 'template_type' ya ha sido registrado.
    """

    def decorator(
        cls: Type["BaseDataRequestTemplate"],
    ) -> Type["BaseDataRequestTemplate"]:
        if template_type in _template_registry:
            raise ValueError(f"Tipo de plantilla '{template_type}' ya está registrado.")
        _template_registry[template_type] = cls
        # Inyecta el 'template_type' como una variable de clase para fácil acceso.
        setattr(cls, "template_type", template_type)
        return cls

    return decorator


class BaseDataRequestTemplate(BaseModel, abc.ABC):
    """
    Clase base abstracta para todas las plantillas de solicitud de datos.

    Define la interfaz común y la lógica de serialización/deserialización
    polimórfica. No debe ser instanciada directamente.
    """

    template_type: ClassVar[str]

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializa la instancia a un diccionario, incluyendo el tipo de plantilla.

        Usa el modo 'json' de Pydantic para asegurar que tipos como `date`
        se conviertan a strings compatibles con JSON.

        Returns:
            Dict[str, Any]: La representación en diccionario del objeto.
        """
        data = self.model_dump(mode="json")
        data["template_type"] = self.template_type
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BaseDataRequestTemplate":
        """
        Deserializa un diccionario a una instancia de la plantilla correcta.

        Utiliza el registro de plantillas para encontrar la clase apropiada
        basándose en la clave 'template_type' del diccionario.

        Args:
            data (Dict[str, Any]): El diccionario a deserializar.

        Returns:
            BaseDataRequestTemplate: Una instancia de la subclase correspondiente.

        Raises:
            ValueError: Si 'template_type' no se encuentra en el diccionario o
                        no corresponde a una plantilla registrada.
        """
        data_copy = data.copy()
        template_type = data_copy.pop("template_type", None)
        if not template_type:
            raise ValueError("El diccionario debe contener 'template_type' para la deserialización.")

        target_class = _template_registry.get(template_type)
        if not target_class:
            raise ValueError(f"Tipo de plantilla desconocido: '{template_type}'")

        return target_class(**data_copy)


@register_template("klines")
class KlinesDataRequestTemplate(BaseDataRequestTemplate):
    """Plantilla para solicitar datos de velas (Klines)."""

    name: str
    symbol: str
    interval: Literal["1m", "5m", "15m", "1h", "4h", "1d", "1w", "1M"]
    start_date: date
    end_date: date
    description: Optional[str] = None

    @field_validator("symbol")
    def symbol_to_uppercase(cls, v: str) -> str:
        """Convierte el símbolo del par de trading a mayúsculas."""
        return v.upper()

    @model_validator(mode="after")
    def validate_dates(self) -> "KlinesDataRequestTemplate":
        """Valida que la fecha de inicio no sea posterior a la fecha de fin."""
        if self.start_date > self.end_date:
            raise ValueError("start_date no puede ser posterior a end_date")
        return self


@register_template("trades")
class TradesDataRequestTemplate(BaseDataRequestTemplate):
    """Plantilla para solicitar datos de transacciones (Trades)."""

    name: str
    symbol: str
    start_date: date
    end_date: date
    description: Optional[str] = None

    @field_validator("symbol")
    def symbol_to_uppercase(cls, v: str) -> str:
        """Convierte el símbolo del par de trading a mayúsculas."""
        return v.upper()

    @model_validator(mode="after")
    def validate_dates(self) -> "TradesDataRequestTemplate":
        """Valida que la fecha de inicio no sea posterior a la fecha de fin."""
        if self.start_date > self.end_date:
            raise ValueError("start_date no puede ser posterior a end_date")
        return self
